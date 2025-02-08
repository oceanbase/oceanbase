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
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_vec_index_builder_util.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"

namespace oceanbase
{
using namespace sql;
using namespace common;
namespace share
{
/*
  预期 index_param_str 是大写的字串
*/
int ObVectorIndexUtil::parser_params_from_string(
    const ObString &index_param_str, ObVectorIndexType index_type, ObVectorIndexParam &param)
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
    const int64_t default_nlist_value = 128;
    const int64_t default_sample_per_nlist_value = 256;

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
          } else if (new_param_value == "OB") {
            param.lib_ = ObVectorIndexAlgorithmLib::VIAL_OB;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index lib", K(ret), K(new_param_value));
          }
        } else if (new_param_name == "TYPE") {
          if (new_param_value == "HNSW") {
            param.type_ = ObVectorIndexAlgorithmType::VIAT_HNSW;
          } else if (new_param_value == "HNSW_SQ") {
            param.type_ = ObVectorIndexAlgorithmType::VIAT_HNSW_SQ;
          } else if (new_param_value == "IVF_FLAT") {
            param.type_ = ObVectorIndexAlgorithmType::VIAT_IVF_FLAT;
          } else if (new_param_value == "IVF_SQ8") {
            param.type_ = ObVectorIndexAlgorithmType::VIAT_IVF_SQ8;
          } else if (new_param_value == "IVF_PQ") {
            param.type_ = ObVectorIndexAlgorithmType::VIAT_IVF_PQ;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index type", K(ret), K(new_param_value));
          }
        } else if (new_param_name == "M") { // here must be ivf_pq or hnsw index
          int64_t int_value = 0;
          if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (ObVectorIndexType::VIT_HNSW_INDEX == index_type) {
            if (int_value >= 5 && int_value <= 64) {
              param.m_ = int_value;
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support vector index m value", K(ret), K(int_value), K(new_param_value));
            }
          } else if (ObVectorIndexType::VIT_IVF_INDEX == index_type) {
            if (int_value > 0) {
              param.m_ = int_value;
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support vector index m value", K(ret), K(int_value), K(new_param_value));
            }
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
        } else if (new_param_name == "NLIST") {
          int64_t int_value = 0;
          if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (int_value >= 1 && int_value < 65536) {
            param.nlist_ = int_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index nlist value", K(ret), K(int_value), K(new_param_value));
          }
        } else if (new_param_name == "SAMPLE_PER_NLIST") {
          int64_t int_value = 0;
          if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (int_value >= 1 && int_value < UINT64_MAX) {
            param.sample_per_nlist_ = int_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index sample_per_nlist value", K(ret), K(int_value), K(new_param_value));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected vector index param name", K(ret), K(new_param_name));
        }
      }
    }
    if (OB_SUCC(ret)) {  // if vector param is not set, use default
      if (index_type == ObVectorIndexType::VIT_HNSW_INDEX) {
        if (param.m_ == 0) {
          param.m_ = default_m_value;
        }
        if (param.ef_construction_ == 0) {
          param.ef_construction_ = default_ef_construction_value;
        }
        if (param.ef_search_ == 0) {
          param.ef_search_ = default_ef_search_value;
        }
        if (param.lib_ == ObVectorIndexAlgorithmLib::VIAL_MAX) {
          param.lib_ = ObVectorIndexAlgorithmLib::VIAL_VSAG;
        }
      } else if (index_type == ObVectorIndexType::VIT_IVF_INDEX) {
        if (param.nlist_ == 0) {
          param.nlist_ = default_nlist_value;
        }
        if (param.sample_per_nlist_ == 0) {
          param.sample_per_nlist_ = default_sample_per_nlist_value;
        }
        if (param.lib_ == ObVectorIndexAlgorithmLib::VIAL_MAX) {
          param.lib_ = ObVectorIndexAlgorithmLib::VIAL_OB;
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support vector index type", K(ret), K(index_type));
      }
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
    ObVectorIndexParam index_param;
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
          ObVectorIndexType index_type = ObVectorIndexType::VIT_MAX;
          if (index_schema->is_vec_ivf_index()) {
            index_type = ObVectorIndexType::VIT_IVF_INDEX;
          } else if (index_schema->is_vec_hnsw_index()) {
            index_type = ObVectorIndexType::VIT_HNSW_INDEX;
          }
          if (OB_FAIL(parser_params_from_string(index_schema->get_index_params(), index_type, index_param))) {
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
    const ObTableSchema &data_table_schema, ObSchemaGetterGuard &schema_guard, const int64_t col_id, bool &is_column_has_vector_index, ObIndexType& index_type)
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
          } else if (col_schema->is_vec_hnsw_vid_column()) {
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
                  index_type = index_table_schema->get_index_type();
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
      } else if (!col_schema->is_vec_hnsw_vector_column() &&
                 !col_schema->is_vec_ivf_center_id_column() &&
                 !(index_table_schema.is_vec_ivfsq8_meta_index() && col_ids.empty()) &&
                 !(index_table_schema.is_vec_ivfpq_pq_centroid_index() && col_ids.empty())) {
        // only need vec_vector column, here skip other column
        // IVF SQ8 meta can not skip by column flag because flag is not persisted
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
      } else if (!col_schema->is_vec_hnsw_vector_column()) {
        // only need vec_type, here skip vec_vid column of delta_buffer_table rowkey column
      } else if (OB_FAIL(get_vector_dim_from_extend_type_info(col_schema->get_extended_type_info(),
                                                              dim))) {
        LOG_WARN("fail to get vector dim", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::get_vector_index_column_dim(
      const ObTableSchema &index_table_schema,
      const ObTableSchema &data_table_schema,
      int64_t &dim)
{
  int ret = OB_SUCCESS;
  ObSArray<uint64_t> all_column_ids;
  dim = 0;
  bool index_without_vector_col = index_table_schema.is_vec_vid_rowkey_type() ||
                                  index_table_schema.is_vec_rowkey_vid_type() ||
                                  index_table_schema.is_vec_ivfflat_rowkey_cid_index() ||
                                  index_table_schema.is_vec_ivfsq8_rowkey_cid_index() ||
                                  index_table_schema.is_vec_ivfpq_code_index() ||
                                  index_table_schema.is_vec_ivfpq_rowkey_cid_index();
  if (!index_table_schema.is_vec_index()) {
    // skip none vector index
  } else if (index_without_vector_col) {
    // skip has no vector column index table
  } else if (OB_FAIL(index_table_schema.get_column_ids(all_column_ids))) {
    LOG_WARN("fail to get all column ids", K(ret), K(index_table_schema));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < all_column_ids.count(); i++) {
      const int64_t column_id = all_column_ids.at(i);
      const ObColumnSchemaV2 *data_col_schema = nullptr;
      ObArray<ObString> extend_type_info;
      if (OB_ISNULL(data_col_schema = data_table_schema.get_column_schema(column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(column_id), K(index_table_schema));
      } else if (!data_col_schema->is_vec_hnsw_vector_column() &&
                 !data_col_schema->is_vec_ivf_center_vector_column() &&
                 !data_col_schema->is_vec_ivf_data_vector_column()) {
      } else if (OB_FAIL(get_vector_dim_from_extend_type_info(data_col_schema->get_extended_type_info(),
                                                              dim))) {
        LOG_WARN("fail to get vector dim", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::has_same_cascaded_col_id(
    const ObTableSchema &data_table_schema,
    const ObColumnSchemaV2 &col_schema,
    const int64_t col_id,
    bool &has_same_col_id)
{
  int ret = OB_SUCCESS;
  has_same_col_id = false;
  // get generated column cascaded column id info
  // (vector index table key, like `c1` in "create table xxx vector index idx(c1)")
  ObArray<uint64_t> cascaded_column_ids;
  // get column_schema from data table using generate column id
  const ObColumnSchemaV2 *ori_col_schema = data_table_schema.get_column_schema(col_schema.get_column_id());
  if (OB_ISNULL(ori_col_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table column", K(ret), K(col_schema.get_column_id()), K(data_table_schema));
  } else if (OB_FAIL(ori_col_schema->get_cascaded_column_ids(cascaded_column_ids))) {
    LOG_WARN("failed to get cascaded column ids", K(ret));
  } else {
    for (int64_t k = 0; OB_SUCC(ret) && k < cascaded_column_ids.count(); ++k) {
      const ObColumnSchemaV2 *cascaded_column = NULL;
      ObString new_col_name;
      if (OB_ISNULL(cascaded_column = data_table_schema.get_column_schema(cascaded_column_ids.at(k)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cascaded column", K(ret));
      } else if (cascaded_column->get_column_id() == col_id) {
        has_same_col_id = true;
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

  if (OB_ISNULL(schema_guard) || !share::schema::is_vec_index(index_type) || !data_table_schema.is_user_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard), K(index_type), K(data_table_schema));
  } else if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
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
      } else if (is_local_vec_ivf_index(index_type)) {
        // when rebuild ivf index, simple_index_infos inclue both new/old index info, we should know by schema version
        // so do not use tid == OB_INVALID_ID as a condition to stop the for loop
        int64_t last_shcema_version = OB_INVALID_ID;
        bool has_same_col_id = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < index_table_schema->get_column_count(); j++) {
          const ObColumnSchemaV2 *col_schema = nullptr;
          if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
          } else if (!col_schema->is_vec_ivf_center_id_column() &&
                    !index_table_schema->is_vec_ivfsq8_meta_index() &&
                    !index_table_schema->is_vec_ivfpq_pq_centroid_index() &&
                    !index_table_schema->is_vec_ivfpq_code_index() &&
                    !index_table_schema->is_vec_ivfpq_rowkey_cid_index()) {
            // NOTE(liyao): Except for the ivf center id, other column ids are not persisted
            //              and can only be filtered by index type
            // only need vec_vector column, here skip other column
          } else if (OB_FAIL(has_same_cascaded_col_id(data_table_schema, *col_schema, col_id, has_same_col_id))) {
            LOG_WARN("fail to do has_same_cascaded_col_id", K(ret), K(col_id));
          } else if (has_same_col_id &&
                    (last_shcema_version == OB_INVALID_ID ||
                      index_table_schema->get_schema_version() > last_shcema_version)) {
            tid = simple_index_infos.at(i).table_id_;
          }
        }
      } else { // delta buffer, index id, index snapshot, we should check cascaded_column by vec_vector col
        bool has_same_col_id = false;
        for (int64_t j = 0; OB_SUCC(ret) && tid == OB_INVALID_ID && j < index_table_schema->get_column_count(); j++) {
          const ObColumnSchemaV2 *col_schema = nullptr;
          if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
          } else if (!col_schema->is_vec_hnsw_vector_column()) {
            // only need vec_vector column, here skip other column
          } else if (OB_FAIL(has_same_cascaded_col_id(data_table_schema, *col_schema, col_id, has_same_col_id))) {
            LOG_WARN("fail to do has_same_cascaded_col_id", K(ret), K(col_id));
          } else if (has_same_col_id) {
            tid = simple_index_infos.at(i).table_id_;
          }
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::check_vec_index_param(
    const uint64_t tenant_id, const ParseNode *option_node,
    common::ObIAllocator &allocator, const ObTableSchema &tbl_schema,
    ObString &index_params, ObString &vec_column_name, ObIndexType &vec_index_type)
{
  int ret = OB_SUCCESS;
  ObString tmp_str;
  int32_t index_param_length = 0;
  const char *index_param_str = nullptr;

  if (OB_ISNULL(option_node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nullptr", KP(option_node));
  } else if (FALSE_IT(index_param_length = option_node->str_len_)) {
  } else if (FALSE_IT(index_param_str = option_node->str_value_)) {
  } else if (OB_UNLIKELY(index_param_length > OB_MAX_INDEX_PARAMS_LENGTH)) {
    ret = common::OB_ERR_TOO_LONG_IDENT;
    LOG_WARN("index params length is beyond limit", K(ret), K(index_param_length));
    LOG_USER_ERROR(OB_ERR_TOO_LONG_IDENT, index_param_length, index_param_str);
  } else if (0 == option_node->str_len_) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("set index param empty is not allowed now", K(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "set index params empty is");
  } else {
    int64_t vector_dim = 0;
    const ObColumnSchemaV2 *col_schema = nullptr;
    if(OB_ISNULL(col_schema = tbl_schema.get_column_schema(vec_column_name))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column schema", K(ret), KP(col_schema));
    } else if (OB_FAIL(ObVectorIndexUtil::get_vector_dim_from_extend_type_info(col_schema->get_extended_type_info(), vector_dim))) {
      LOG_WARN("fail to get vector dim", K(ret), K(col_schema));
    } else {
      tmp_str.assign_ptr(index_param_str, index_param_length);
      if (OB_ISNULL(option_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("children can't be null", K(ret));
      } else if (OB_FAIL(ObVectorIndexUtil::insert_index_param_str(tmp_str, allocator, index_params))) {
        LOG_WARN("write string failed", K(ret), K(tmp_str), K(index_params));
      } else if (OB_FAIL(check_index_param(option_node, allocator, vector_dim, index_params, vec_index_type))) {
        LOG_WARN("fail to check vector index definition", K(ret));
      } else if (share::schema::is_vec_hnsw_index(vec_index_type)) {
        bool is_vector_memory_valid = false;
        if (OB_FAIL(ObPluginVectorIndexHelper::is_ob_vector_memory_valid(tenant_id, is_vector_memory_valid))) {
          LOG_WARN("fail to check is_ob_vector_memory_valid", K(ret));
        } else if (!is_vector_memory_valid) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support vector index when ob_vector_memory_limit_percentage is 0", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "when ob_vector_memory_limit_percentage = 0 or memstore_limit >= 85, vector index is");
        }
      }
    }
  }
  return ret;
}

// for ivf
int ObVectorIndexUtil::get_vector_index_tid(
    sql::ObSqlSchemaGuard *schema_guard,
    const ObTableSchema &data_table_schema,
    const ObIndexType index_type,
    const int64_t vec_cid_col_id,
    uint64_t &tid)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  tid = OB_INVALID_ID;

  if (OB_ISNULL(schema_guard) || !share::schema::is_vec_index(index_type) || !data_table_schema.is_user_table() || OB_INVALID_ID == vec_cid_col_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard), K(index_type), K(data_table_schema));
  } else if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count() && tid == OB_INVALID_ID; ++i) {
      const ObTableSchema *index_table_schema = nullptr;
      if (OB_FAIL(schema_guard->get_table_schema(simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("fail to get index_table_schema", K(ret), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (!index_table_schema->is_vec_index()) {
        // skip none vector index
      } else if (index_table_schema->get_index_type() != index_type) {
        // skip not spec index type
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && tid == OB_INVALID_ID && j < index_table_schema->get_column_count(); j++) {
          const ObColumnSchemaV2 *col_schema = nullptr;
          if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
          } else if (!col_schema->is_vec_ivf_center_id_column()) {
          } else if (vec_cid_col_id == col_schema->get_column_id()) {
            tid = index_table_schema->get_table_id();
          }
        }
      }
    }
  }
  return ret;
}
/*
  NOTE: Only one vector index can be created on the same column now
 */
int ObVectorIndexUtil::get_vector_index_param(
    share::schema::ObSchemaGetterGuard *schema_guard,
    const ObTableSchema &data_table_schema,
    const int64_t col_id,
    ObVectorIndexParam &param)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  bool filled = false;
  if (OB_ISNULL(schema_guard) || !data_table_schema.is_user_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard), K(data_table_schema));
  } else if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count() && !filled; ++i) {
      const ObTableSchema *index_table_schema = nullptr;
      if (OB_FAIL(schema_guard->get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("fail to get index_table_schema", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (!index_table_schema->is_vec_index()) {
        // skip none vector index
      } else if (index_table_schema->is_built_in_vec_index()) {
        // skip built in vec index
      } else { // we should check cascaded_column by vec_vector col
        for (int64_t j = 0; OB_SUCC(ret) && j < index_table_schema->get_column_count() && !filled; j++) {
          const ObColumnSchemaV2 *col_schema = nullptr;
          if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
          } else if (!col_schema->is_vec_hnsw_vector_column() &&
                     !col_schema->is_vec_ivf_center_id_column() &&
                     !index_table_schema->is_vec_ivfsq8_meta_index()) {
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
              for (int64_t k = 0; OB_SUCC(ret) && k < cascaded_column_ids.count() && !filled; ++k) {
                const ObColumnSchemaV2 *cascaded_column = NULL;
                ObString new_col_name;
                if (OB_ISNULL(cascaded_column = data_table_schema.get_column_schema(cascaded_column_ids.at(k)))) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unexpected cascaded column", K(ret));
                } else if (cascaded_column->get_column_id() == col_id) {
                  ObVectorIndexType index_type = ObVectorIndexType::VIT_MAX;
                  if (index_table_schema->is_vec_ivf_index()) {
                    index_type = ObVectorIndexType::VIT_IVF_INDEX;
                  } else if (index_table_schema->is_vec_hnsw_index()) {
                    index_type = ObVectorIndexType::VIT_HNSW_INDEX;
                  }
                  if (OB_FAIL(parser_params_from_string(index_table_schema->get_index_params(), index_type, param))) {
                    LOG_WARN("fail to parser params from string", K(ret), K(index_table_schema->get_index_params()));
                  } else {
                    filled = true;
                  }
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

int ObVectorIndexUtil::check_index_param(
    const ParseNode *option_node, common::ObIAllocator &allocator,
    const int64_t vector_dim, ObString &index_params,  ObIndexType &out_index_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(option_node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse node", K(ret), KP(option_node));
  } else if (option_node->type_ != T_VEC_INDEX_PARAMS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse node type", K(ret), K(option_node->type_));
  } else if (OB_ISNULL(option_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("option_node child is null", K(ret), KP(option_node->children_[0]));
  } else if (vector_dim <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector dim", K(ret), K(vector_dim));
  } else {
    if (option_node->num_child_ < 4 || option_node->num_child_ % 2 !=  0) {  // at least distance and type should be set
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid vector param num", K(ret), K(option_node->num_child_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "vector index params not set distance and type is");
    }
    ObString last_variable;
    ObString key_parser_name;
    ObString data_parser_name;
    ObString new_variable_name;
    ObString new_parser_name;
    ObString lib_name;
    int64_t parser_value = 0;
    int32_t str_len = 0;
    int64_t m_value = 0;
    int64_t ef_construction_value = 0;
    int64_t sample_per_nlist_value = 0;
    int64_t nlist_value = 0;

    bool distance_is_set = false;       // ivf/hnsw
    bool lib_is_set = false;            // ivf/hnsw
    bool type_hnsw_is_set = false;      // ivf/hnsw
    bool m_is_set = false;              // ivf/hnsw
    bool ef_construction_is_set = false;// hnsw
    bool ef_search_is_set = false;      // hnsw
    bool nlist_is_set = false;          // ivf
    bool sample_per_nlist_is_set = false; // ivf
    bool type_ivf_flat_is_set = false;  // ivf
    bool type_ivf_sq8_is_set = false;   // ivf
    bool type_ivf_pq_is_set = false;    // ivf

    const int64_t default_m_value = 16;
    const int64_t default_ef_construction_value = 200;
    const int64_t default_ef_search_value = 64;
    const int64_t default_nlist_value = 128;
    const int64_t default_sample_per_nlist_value = 256;
    hash::ObHashSet<ObString> param_set;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(param_set.create(option_node->num_child_, lib::ObMemAttr(MTL_ID(), "VecParamSet")))) {
      LOG_WARN("fail to create param hash set", K(ret), K(option_node->num_child_));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < option_node->num_child_; ++i) {
      int32_t child_node_index = i % 2;
      if (child_node_index == 0) {
        str_len = static_cast<int32_t>(option_node->children_[i]->str_len_);
        key_parser_name.assign_ptr(option_node->children_[i]->str_value_, str_len);
        new_variable_name = key_parser_name;
        if (OB_FAIL(ob_simple_low_to_up(allocator, key_parser_name, new_variable_name))) {
          LOG_WARN("string low to up failed", K(ret), K(key_parser_name));
        } else if (new_variable_name != "DISTANCE" &&
                   new_variable_name != "LIB" &&
                   new_variable_name != "TYPE" &&
                   new_variable_name != "M" &&
                   new_variable_name != "EF_CONSTRUCTION" &&
                   new_variable_name != "EF_SEARCH" &&
                   new_variable_name != "NLIST" &&
                   new_variable_name != "SAMPLE_PER_NLIST") {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unexpected vector variable name", K(ret), K(new_variable_name));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "unexpected vector index params items is");
        } else if (OB_FAIL(param_set.set_refactored(new_variable_name, 0/*flag*/))) {
          if (ret == OB_HASH_EXIST) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support duplicate param", K(ret), K(new_variable_name));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "duplicate param is");
          } else {
            LOG_WARN("fail to do ObHashSet::set", K(ret), K(new_variable_name));
          }
        } else {
          last_variable = new_variable_name;
        }
      } else {
        if (option_node->children_[i]->type_ == T_NUMBER) {
          parser_value = option_node->children_[i]->value_;
        } else {
          str_len = static_cast<int32_t>(option_node->children_[i]->str_len_);
          data_parser_name.assign_ptr(option_node->children_[i]->str_value_, str_len);
          new_parser_name = data_parser_name;
          if (OB_FAIL(ob_simple_low_to_up(allocator, data_parser_name, new_parser_name))) {
            LOG_WARN("string low to up failed", K(ret), K(data_parser_name));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (last_variable == "DISTANCE") {
          if (new_parser_name == "INNER_PRODUCT" ||
              new_parser_name == "L2" ||
              new_parser_name == "COSINE") {
            distance_is_set = true;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index distance algorithm", K(ret), K(new_parser_name));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this type of vector index distance algorithm is");
          }
        } else if (last_variable == "LIB") {
          if (new_parser_name == "VSAG" ||
              new_parser_name == "OB") {
            lib_is_set = true;
            lib_name = new_parser_name;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index lib", K(ret), K(new_parser_name));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this type of vector index lib is");
          }
        } else if (last_variable == "TYPE") {
          if (new_parser_name == "HNSW") {
            type_hnsw_is_set = true;
          } else if (new_parser_name == "HNSW_SQ") {
            type_hnsw_is_set = true;
          } else if (new_parser_name == "IVF_FLAT") {
            type_ivf_flat_is_set = true;
          } else if (new_parser_name == "IVF_SQ8") {
            type_ivf_sq8_is_set = true;
          } else if (new_parser_name == "IVF_PQ") {
            type_ivf_pq_is_set = true;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index type", K(ret), K(new_parser_name));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this type of vector index type is");
          }
        } else if (last_variable == "M") {  // check "m" later
          m_is_set = true;
          m_value = parser_value;
        } else if (last_variable == "EF_CONSTRUCTION") {
          if (parser_value >= 5 && parser_value <= 1000 ) {
            ef_construction_is_set = true;
            ef_construction_value = parser_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid vector index ef_construction value", K(ret), K(parser_value));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index ef_construction is");
          }
        } else if (last_variable == "EF_SEARCH") {
          if (parser_value >= 1 && parser_value <= 1000 ) {
            ef_search_is_set = true;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid vector index ef_search value", K(ret), K(parser_value));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index ef_search is");
          }
        } else if (last_variable == "NLIST") {
          if (parser_value >= 1 && parser_value <= 65535 ) {
            nlist_is_set = true;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid vector index nlist value", K(ret), K(parser_value));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index nlist is");
          }
        } else if (last_variable == "SAMPLE_PER_NLIST") {
          if (parser_value >= 1 && parser_value <= UINT64_MAX ) {
            sample_per_nlist_is_set = true;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid vector index sample_per_nlist value", K(ret), K(parser_value));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index sample_per_nlist is");
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support vector index param", K(ret), K(last_variable));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index ef_search is");
        }
      }
    }
    if (OB_SUCC(ret)) {
      bool ivf_is_set = type_ivf_sq8_is_set || type_ivf_pq_is_set || type_ivf_flat_is_set;
      bool hnsw_is_set = type_hnsw_is_set;
      if (!distance_is_set || !(ivf_is_set || hnsw_is_set)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unexpected setting of vector index param, distance or type has not been set",
          K(ret), K(distance_is_set), K(hnsw_is_set), K(ivf_is_set));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "the vector index params of distance or type not set is");
      } else if (ivf_is_set) {
        if (lib_is_set && lib_name != "OB") {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ivf vector index name should be 'OB'", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ivf vector index lib name not equal to 'OB' is");
        }
        if (OB_FAIL(ret)) {
        } else if (!type_ivf_pq_is_set && m_is_set) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ivf vector index param m only need to be set of ivf_pq mode", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ivf vector index param m to be set in ivf_sq8 or ivf_flat is");
        }
        if (OB_FAIL(ret)) {
        } else if (type_ivf_pq_is_set && !m_is_set) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ivf_pq vector index param m needs to be set", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ivf_pq vector index param m not set is");
        }
        if (OB_FAIL(ret)) {
        } else if (type_ivf_pq_is_set && m_value == 0) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ivf vector index param m have to large than zero", K(ret), K(m_value));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index m equal to zero is");
        } else if (type_ivf_pq_is_set && (vector_dim % m_value != 0 || vector_dim < m_value)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ivf vector index param m needs to be divisible by dim, or less than dim", K(ret), K(vector_dim), K(m_value));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index m not to be divisible by dim or greater than dim is");
        }
        if (OB_FAIL(ret)) {
        } else if (ef_construction_is_set || ef_search_is_set) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ivf vector index param ef_construction or ef_search should not be set", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ivf vector index param setting ef_construction or ef_search should is");
        }
        nlist_value = nlist_is_set ? nlist_value : default_nlist_value;
        sample_per_nlist_value = sample_per_nlist_is_set ? sample_per_nlist_value : default_sample_per_nlist_value;
      } else if (hnsw_is_set) {
        ef_construction_value = ef_construction_is_set ? ef_construction_value : default_ef_construction_value;
        m_value = m_is_set ? m_value : default_m_value;
        if (m_value >= 5 && m_value <= 64) {
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("invalid vector index m value", K(ret), K(parser_value));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index m is");
        }
        if (OB_FAIL(ret)) {
        } else if (lib_is_set && lib_name != "VSAG") {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("hnsw vector index name should be 'VSAG'", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "hnsw vector index lib name not equal to 'VSAG' is");
        }
        if (OB_FAIL(ret)) {
        } else if (ef_construction_value <= m_value) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("unexpected setting of vector index param, ef_construction value must be larger than m value",
            K(ret), K(ef_construction_value), K(m_value));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "the vector index params ef_construction less than or equal to m value is");
        }
        if (OB_FAIL(ret)) {
        } else if (nlist_is_set || sample_per_nlist_is_set) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("hnsw vector index no need to set nlist or sample_per_nlist",
            K(ret), K(nlist_is_set), K(sample_per_nlist_is_set));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "hnsw vector index setting nlist or sample_per_nlist is");
        }
      }
      if (OB_SUCC(ret)) {
        char not_set_params_str[OB_MAX_TABLE_NAME_LENGTH];
        const ObString default_lib = ivf_is_set ? "OB" : "VSAG";
        int64_t pos = 0;
        if (!lib_is_set && OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
                                                    ", LIB=%.*s", default_lib.length(), default_lib.ptr()))) {
          LOG_WARN("fail to printf databuff", K(ret));
        } else if (!m_is_set && !ivf_is_set &&
                   OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
                                           ", M=%ld", default_m_value))) {
          LOG_WARN("fail to printf databuff", K(ret));
        } else if (!ef_construction_is_set && !ivf_is_set &&
                   OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
                                           ", EF_CONSTRUCTION=%ld", default_ef_construction_value))) {
          LOG_WARN("fail to printf databuff", K(ret));
        } else if (!ef_search_is_set && !ivf_is_set &&
                   OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
                                           ", EF_SEARCH=%ld", default_ef_search_value))) {
          LOG_WARN("fail to printf databuff", K(ret));
        } else if (!nlist_is_set && !hnsw_is_set &&
                   OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
                                           ", NLIST=%ld", default_nlist_value))) {
          LOG_WARN("fail to printf databuff", K(ret));
        } else if (!sample_per_nlist_is_set && !hnsw_is_set &&
                   OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
                                           ", SAMPLE_PER_NLIST=%ld", default_sample_per_nlist_value))) {
          LOG_WARN("fail to printf databuff", K(ret));
        } else {
          char *buf = nullptr;
          const int64_t alloc_len = index_params.length() + pos;
          if (OB_ISNULL(buf = (static_cast<char *>(allocator.alloc(alloc_len))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc memory for vector index param", K(ret), K(alloc_len));
          } else {
            MEMCPY(buf, index_params.ptr(), index_params.length());
            MEMCPY(buf + index_params.length(), not_set_params_str, pos);
            index_params.assign_ptr(buf, alloc_len);
            LOG_DEBUG("vector index params", K(index_params));
          }
        }
      }
    }
    out_index_type = INDEX_TYPE_MAX;
    if (OB_FAIL(ret)) {
    } else if (type_hnsw_is_set) {
      out_index_type = INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL;
    } else if (type_ivf_flat_is_set) {
      out_index_type = INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL;
    } else if (type_ivf_sq8_is_set) {
      out_index_type = INDEX_TYPE_VEC_IVFSQ8_CENTROID_LOCAL;
    } else if (type_ivf_pq_is_set) {
      out_index_type = INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vec index type is set", K(ret));
    }
  }

  return ret;
}

int ObVectorIndexUtil::get_vector_index_type(
    sql::ObRawExpr *&raw_expr,
    const ObVectorIndexParam &param,
    ObIArray<ObIndexType> &type_array)
{
  int ret = OB_SUCCESS;
  if (VIAT_MAX == param.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param));
  } else if (T_FUN_SYS_VEC_IVF_CENTER_ID == raw_expr->get_expr_type()) {
    if (VIAT_IVF_FLAT == param.type_) {
      if (OB_FAIL(type_array.push_back(INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL))) {
        LOG_WARN("failed to push back array", K(ret));
      }
    } else if (VIAT_IVF_SQ8 == param.type_) {
      if (OB_FAIL(type_array.push_back(INDEX_TYPE_VEC_IVFSQ8_CENTROID_LOCAL))) {
        LOG_WARN("failed to push back array", K(ret));
      }
    } else if (VIAT_IVF_PQ == param.type_) {
      if (OB_FAIL(type_array.push_back(INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL))) {
        LOG_WARN("failed to push back array", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(param));
    }
  } else if (T_FUN_SYS_VEC_IVF_PQ_CENTER_IDS == raw_expr->get_expr_type()) {
    if (VIAT_IVF_PQ == param.type_) {
      if (OB_FAIL(type_array.push_back(INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL))) {
        LOG_WARN("failed to push back array", K(ret));
      } else if (OB_FAIL(type_array.push_back(INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL))) {
        LOG_WARN("failed to push back array", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(param));
    }
  } else if (T_FUN_SYS_VEC_IVF_SQ8_DATA_VECTOR == raw_expr->get_expr_type()) {
    if (VIAT_IVF_SQ8 == param.type_) {
      if (OB_FAIL(type_array.push_back(INDEX_TYPE_VEC_IVFSQ8_META_LOCAL))) {
        LOG_WARN("failed to push back array", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(param));
    }
  } else if (T_FUN_SYS_VEC_IVF_PQ_CENTER_VECTOR == raw_expr->get_expr_type()) {
    if (VIAT_IVF_PQ == param.type_) {
      if (OB_FAIL(type_array.push_back(INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL))) {
        LOG_WARN("failed to push back array", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), K(param));
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
    const ObIndexType index_type,
    ObIAllocator &allocator,
    ObIArray<ObString> &old_table_names,
    ObIArray<ObString> &new_table_names)
{
  int ret = OB_SUCCESS;
  if (share::schema::is_vec_hnsw_index(index_type)) {
    if (OB_FAIL(generate_hnsw_switch_index_names(old_domain_index_name,
                                                 new_domain_index_name,
                                                 allocator,
                                                 old_table_names,
                                                 new_table_names))) {
      LOG_WARN("fail to generate hnsw swith index names", K(ret));
    }
  } else if (share::schema::is_vec_ivfflat_index(index_type)) {
    if (OB_FAIL(generate_ivfflat_switch_index_names(old_domain_index_name,
                                                 new_domain_index_name,
                                                 allocator,
                                                 old_table_names,
                                                 new_table_names))) {
      LOG_WARN("fail to generate ivfflat swith index names", K(ret));
    }
  } else if (share::schema::is_vec_ivfsq8_index(index_type)) {
    if (OB_FAIL(generate_ivfsq8_switch_index_names(old_domain_index_name,
                                                 new_domain_index_name,
                                                 allocator,
                                                 old_table_names,
                                                 new_table_names))) {
      LOG_WARN("fail to generate ivfsq8 swith index names", K(ret));
    }
  } else if (share::schema::is_vec_ivfpq_index(index_type)) {
    if (OB_FAIL(generate_ivfpq_switch_index_names(old_domain_index_name,
                                                 new_domain_index_name,
                                                 allocator,
                                                 old_table_names,
                                                 new_table_names))) {
      LOG_WARN("fail to generate ivfpq swith index names", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index type", K(ret), K(index_type));
  }
  return ret;
}


int ObVectorIndexUtil::generate_hnsw_switch_index_names(
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


int ObVectorIndexUtil::generate_ivfflat_switch_index_names(
    const ObString &old_domain_index_name,
    const ObString &new_domain_index_name,
    ObIAllocator &allocator,
    ObIArray<ObString> &old_table_names,
    ObIArray<ObString> &new_table_names)
{
  int ret = OB_SUCCESS;
  ObString old_centroid_table_name = old_domain_index_name;
  ObString new_centroid_table_name = new_domain_index_name;
  ObString new_cid_vector_table_name;
  ObString new_rowkey_cid_table_name;
  ObString old_cid_vector_table_name;
  ObString old_rowkey_cid_table_name;

  if (OB_FAIL(new_table_names.push_back(new_centroid_table_name))) {
    LOG_WARN("fail to push back new centroid table name", K(ret));
  } else if (OB_FAIL(old_table_names.push_back(old_centroid_table_name))) {
    LOG_WARN("fail to push back old centroid table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFFLAT_CID_VECTOR_LOCAL,
                                                                    new_domain_index_name,
                                                                    new_cid_vector_table_name))) {
    LOG_WARN("fail to generate new cid vector table name", K(ret), K(new_domain_index_name));
  } else if (OB_FAIL(new_table_names.push_back(new_cid_vector_table_name))) {
    LOG_WARN("fail to push back new cid vector table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFFLAT_CID_VECTOR_LOCAL,
                                                                    old_domain_index_name,
                                                                    old_cid_vector_table_name))) {
    LOG_WARN("fail to generate old cid vector table name", K(ret), K(old_domain_index_name));
  } else if (OB_FAIL(old_table_names.push_back(old_cid_vector_table_name))) {
    LOG_WARN("fail to push back old cid vector table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFFLAT_ROWKEY_CID_LOCAL,
                                                                    new_domain_index_name,
                                                                    new_rowkey_cid_table_name))) {
    LOG_WARN("fail to generate new rowkey cid table name", K(ret), K(new_domain_index_name));
  } else if (OB_FAIL(new_table_names.push_back(new_rowkey_cid_table_name))) {
    LOG_WARN("fail to push back new rowkey cid table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFFLAT_ROWKEY_CID_LOCAL,
                                                                    old_domain_index_name,
                                                                    old_rowkey_cid_table_name))) {
    LOG_WARN("fail to generate old rowkey cid table name", K(ret), K(old_domain_index_name));
  } else if (OB_FAIL(old_table_names.push_back(old_rowkey_cid_table_name))) {
    LOG_WARN("fail to push back old rowkey cid table name", K(ret));
  }
  return ret;
}

int ObVectorIndexUtil::generate_ivfsq8_switch_index_names(
    const ObString &old_domain_index_name,
    const ObString &new_domain_index_name,
    ObIAllocator &allocator,
    ObIArray<ObString> &old_table_names,
    ObIArray<ObString> &new_table_names)
{
  int ret = OB_SUCCESS;
  ObString old_centroid_table_name = old_domain_index_name;
  ObString new_centroid_table_name = new_domain_index_name;
  ObString new_sq_meta_table_name;
  ObString new_cid_vector_table_name;
  ObString new_rowkey_cid_table_name;
  ObString old_sq_meta_table_name;
  ObString old_cid_vector_table_name;
  ObString old_rowkey_cid_table_name;

  if (OB_FAIL(new_table_names.push_back(new_centroid_table_name))) {
    LOG_WARN("fail to push back new centroid table name", K(ret));
  } else if (OB_FAIL(old_table_names.push_back(old_centroid_table_name))) {
    LOG_WARN("fail to push back old centroid table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFSQ8_CID_VECTOR_LOCAL,
                                                                    new_domain_index_name,
                                                                    new_cid_vector_table_name))) {
    LOG_WARN("fail to generate new cid vector table name", K(ret), K(new_domain_index_name));
  } else if (OB_FAIL(new_table_names.push_back(new_cid_vector_table_name))) {
    LOG_WARN("fail to push back new cid vector table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFSQ8_CID_VECTOR_LOCAL,
                                                                    old_domain_index_name,
                                                                    old_cid_vector_table_name))) {
    LOG_WARN("fail to generate old cid vector table name", K(ret), K(old_domain_index_name));
  } else if (OB_FAIL(old_table_names.push_back(old_cid_vector_table_name))) {
    LOG_WARN("fail to push back old cid vector table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFSQ8_ROWKEY_CID_LOCAL,
                                                                    new_domain_index_name,
                                                                    new_rowkey_cid_table_name))) {
    LOG_WARN("fail to generate new rowkey cid table name", K(ret), K(new_domain_index_name));
  } else if (OB_FAIL(new_table_names.push_back(new_rowkey_cid_table_name))) {
    LOG_WARN("fail to push back new rowkey cid table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFSQ8_ROWKEY_CID_LOCAL,
                                                                    old_domain_index_name,
                                                                    old_rowkey_cid_table_name))) {
    LOG_WARN("fail to generate old rowkey cid table name", K(ret), K(old_domain_index_name));
  } else if (OB_FAIL(old_table_names.push_back(old_rowkey_cid_table_name))) {
    LOG_WARN("fail to push back old rowkey cid table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFSQ8_META_LOCAL,
                                                                    new_domain_index_name,
                                                                    new_sq_meta_table_name))) {
    LOG_WARN("fail to generate new sq meta table name", K(ret), K(new_domain_index_name));
  } else if (OB_FAIL(new_table_names.push_back(new_sq_meta_table_name))) {
    LOG_WARN("fail to push back new sq meta table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFSQ8_META_LOCAL,
                                                                    old_domain_index_name,
                                                                    old_sq_meta_table_name))) {
    LOG_WARN("fail to generate old sq meta table name", K(ret), K(old_domain_index_name));
  } else if (OB_FAIL(old_table_names.push_back(old_sq_meta_table_name))) {
    LOG_WARN("fail to push back old sq meta table name", K(ret));
  }

  return ret;
}


int ObVectorIndexUtil::generate_ivfpq_switch_index_names(
    const ObString &old_domain_index_name,
    const ObString &new_domain_index_name,
    ObIAllocator &allocator,
    ObIArray<ObString> &old_table_names,
    ObIArray<ObString> &new_table_names)
{
  int ret = OB_SUCCESS;
  ObString old_centroid_table_name = old_domain_index_name;
  ObString new_centroid_table_name = new_domain_index_name;
  ObString new_pq_centroid_table_name;
  ObString new_pq_code_table_name;
  ObString new_pq_rowkey_cid_table_name;
  ObString old_pq_centroid_table_name;
  ObString old_pq_code_table_name;
  ObString old_pq_rowkey_cid_table_name;

  if (OB_FAIL(new_table_names.push_back(new_centroid_table_name))) {
    LOG_WARN("fail to push back new centroid table name", K(ret));
  } else if (OB_FAIL(old_table_names.push_back(old_centroid_table_name))) {
    LOG_WARN("fail to push back old centroid table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL,
                                                                    new_domain_index_name,
                                                                    new_pq_centroid_table_name))) {
    LOG_WARN("fail to generate new pq centroid table name", K(ret), K(new_domain_index_name));
  } else if (OB_FAIL(new_table_names.push_back(new_pq_centroid_table_name))) {
    LOG_WARN("fail to push back new pq centroid table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL,
                                                                    old_domain_index_name,
                                                                    old_pq_centroid_table_name))) {
    LOG_WARN("fail to generate old pq centroid table name", K(ret), K(old_domain_index_name));
  } else if (OB_FAIL(old_table_names.push_back(old_pq_centroid_table_name))) {
    LOG_WARN("fail to push back old pq centroid table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFPQ_CODE_LOCAL,
                                                                    new_domain_index_name,
                                                                    new_pq_code_table_name))) {
    LOG_WARN("fail to generate nsw pq code table name", K(ret), K(new_domain_index_name));
  } else if (OB_FAIL(new_table_names.push_back(new_pq_code_table_name))) {
    LOG_WARN("fail to push back new pq code table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFPQ_CODE_LOCAL,
                                                                    old_domain_index_name,
                                                                    old_pq_code_table_name))) {
    LOG_WARN("fail to generate old pq code table name", K(ret), K(old_domain_index_name));
  } else if (OB_FAIL(old_table_names.push_back(old_pq_code_table_name))) {
    LOG_WARN("fail to push back old pq code table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFPQ_ROWKEY_CID_LOCAL,
                                                                    new_domain_index_name,
                                                                    new_pq_rowkey_cid_table_name))) {
    LOG_WARN("fail to generate new pq rowkey cid table name", K(ret), K(new_domain_index_name));
  } else if (OB_FAIL(new_table_names.push_back(new_pq_rowkey_cid_table_name))) {
    LOG_WARN("fail to push back new pq rowkey cid table name", K(ret));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::generate_vec_index_name(&allocator,
                                                                    INDEX_TYPE_VEC_IVFPQ_ROWKEY_CID_LOCAL,
                                                                    old_domain_index_name,
                                                                    old_pq_rowkey_cid_table_name))) {
    LOG_WARN("fail to generate old pq rowkey cid table name", K(ret), K(old_domain_index_name));
  } else if (OB_FAIL(old_table_names.push_back(old_pq_rowkey_cid_table_name))) {
    LOG_WARN("fail to push back old pq rowkey cid table name", K(ret));
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

  bool is_hnsw = false;
  bool vid_rowkey_table_is_valid = false;
  bool rowkey_vid_table_is_valid = false;
  bool delta_buffer_table_is_valid = false;
  bool index_id_table_is_valid = false;
  bool snapshot_table_is_valid = false;

  bool is_ivf = false;
  bool ivf_first_table_is_valid = false;
  bool ivf_second_table_is_valid = false;
  bool ivf_third_table_is_valid = false;
  bool ivf_forth_table_is_valid = true;

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
      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count() && !is_ivf; ++i) {
        const ObTableSchema *index_schema = nullptr;
        const int64_t table_id = simple_index_infos.at(i).table_id_;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, index_schema))) {
          LOG_WARN("fail to get index table schema", K(ret), K(tenant_id), K(table_id));
        } else if (OB_ISNULL(index_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
        } else if (!index_schema->is_vec_index()) {
          // skip none vector index
        } else if (index_schema->is_vec_hnsw_index()) {
          if (index_schema->is_vec_rowkey_vid_type()) {
            rowkey_vid_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
          } else if (index_schema->is_vec_vid_rowkey_type()) {
            vid_rowkey_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
          } else if (index_schema->is_vec_delta_buffer_type()) {
            if (!is_match_index_column_name(table_schema, *index_schema, index_column_name)) {
              // skip
            } else {
              is_hnsw = true;
              delta_buffer_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
            }
          } else if (index_schema->is_vec_index_id_type()) {
            if (!is_match_index_column_name(table_schema, *index_schema, index_column_name)) {
              // skip
            } else {
              is_hnsw = true;
              index_id_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
            }
          } else if (index_schema->is_vec_index_snapshot_data_type()) {
            if (!is_match_index_column_name(table_schema, *index_schema, index_column_name)) {
              // skip
            } else {
              is_hnsw = true;
              snapshot_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
            }
          }
        } else if (index_schema->is_vec_ivf_index()) {
          if (index_schema->is_vec_domain_index()) {
            if (!is_match_index_column_name(table_schema, *index_schema, index_column_name)) {
              // skip
            } else {
              is_ivf = true;
              ivf_first_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
            }
          } else if (index_schema->is_vec_ivfflat_cid_vector_index() ||
                     index_schema->is_vec_ivfsq8_meta_index() ||
                     index_schema->is_vec_ivfpq_pq_centroid_index()) {
            ivf_second_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
          } else if (index_schema->is_vec_ivfflat_rowkey_cid_index() ||
                     index_schema->is_vec_ivfsq8_cid_vector_index() ||
                     index_schema->is_vec_ivfpq_code_index()) {
            ivf_third_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
          } else if (index_schema->is_vec_ivfsq8_rowkey_cid_index() || index_schema->is_vec_ivfpq_rowkey_cid_index()) {
            ivf_forth_table_is_valid = index_schema->can_read_index() || !index_schema->is_index_visible();
          }
        }
      }

      if(OB_FAIL(ret)){
      } else if (is_hnsw && is_ivf) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("only one vector index can be created on a vector column.", K(ret),
                  K(index_column_name), K(data_table_id), K(tenant_id), K(database_id));
      } else if (is_hnsw) {
        if (rowkey_vid_table_is_valid && vid_rowkey_table_is_valid && delta_buffer_table_is_valid &&
            index_id_table_is_valid && snapshot_table_is_valid) {
          is_valid = true;
        } else {
          LOG_WARN("vector index is not all valid",
                   K(rowkey_vid_table_is_valid),
                   K(vid_rowkey_table_is_valid),
                   K(delta_buffer_table_is_valid),
                   K(index_id_table_is_valid),
                   K(snapshot_table_is_valid));
        }
      } else if (is_ivf) {
        if (ivf_first_table_is_valid && ivf_second_table_is_valid && ivf_third_table_is_valid && ivf_forth_table_is_valid) {
          is_valid = true;
        } else {
          LOG_WARN("vector index is not all valid",
                  K(ivf_first_table_is_valid),
                  K(ivf_second_table_is_valid),
                  K(ivf_third_table_is_valid),
                  K(ivf_forth_table_is_valid));
        }
      }
    }
  }
  LOG_INFO("check_vector_index_by_column_name", K(is_valid), K(ret));
  return ret;
}

/*
  1. hnsw索引的vector列是持久化列，可以从该colum_schema获取cascaded column，从而获取到索引列名字
  2. ivf索引的centroid列是持久化列，可以从该column_schema中获取cascaded column，从而获取到索引列名字
  3. 目前只支持单列向量索引,返回的col_names.count=1
*/
int ObVectorIndexUtil::get_vector_index_column_name(
    const ObTableSchema &data_table_schema, const ObTableSchema &index_table_schema, ObIArray<ObString> &col_names)
{
  INIT_SUCC(ret);
  col_names.reset();
  bool has_get_column_name = false;
  if (!index_table_schema.is_vec_index()) {
    // skip none vector index
  } else if (index_table_schema.is_vec_rowkey_vid_type() || index_table_schema.is_vec_vid_rowkey_type()) {
    // skip rowkey_vid and vid_rowkey table
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && !has_get_column_name && i < index_table_schema.get_column_count(); i++) {
      const ObColumnSchemaV2 *col_schema = nullptr;
      if (OB_ISNULL(col_schema = index_table_schema.get_column_schema_by_idx(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(i), K(index_table_schema));
      } else {
        // get generated column cascaded column id info
        // (vector index table key, like `c1` in "create table xxx vector index idx(c1)")
        ObArray<uint64_t> cascaded_column_ids;
        // get column_schema from data table using generate column id
        const ObColumnSchemaV2 *ori_col_schema = data_table_schema.get_column_schema(col_schema->get_column_id());
        if (OB_ISNULL(ori_col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected ori column", K(ret), K(col_schema->get_column_id()), K(data_table_schema));
        } else if (!ori_col_schema->is_vec_hnsw_vector_column() && !ori_col_schema->is_vec_ivf_center_vector_column()) {
          // only need vec_vector column, here skip other column
        } else if (OB_FAIL(ori_col_schema->get_cascaded_column_ids(cascaded_column_ids))) {
          LOG_WARN("failed to get cascaded column ids", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && !has_get_column_name && j < cascaded_column_ids.count(); ++j) {
            const ObColumnSchemaV2 *cascaded_column = NULL;
            ObString new_col_name;
            if (OB_ISNULL(cascaded_column = data_table_schema.get_column_schema(cascaded_column_ids.at(j)))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected cascaded column", K(ret));
            } else if (OB_FALSE_IT(new_col_name = cascaded_column->get_column_name())) {
            } else if (OB_FAIL(col_names.push_back(new_col_name))) {
              LOG_WARN("fail to push back col names", K(ret), K(new_col_name));
            } else {
              has_get_column_name = true;
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

bool ObVectorIndexUtil::check_is_match_index_type(const ObIndexType type1, const ObIndexType type2)
{
  bool is_match = false;
  if (share::schema::is_vec_hnsw_index(type1) && share::schema::is_vec_hnsw_index(type2)) {
    is_match = true;
  } else if (share::schema::is_vec_ivfflat_index(type1) && share::schema::is_vec_ivfflat_index(type2)) {
    is_match = true;
  } else if (share::schema::is_vec_ivfsq8_index(type1) && share::schema::is_vec_ivfsq8_index(type2)) {
    is_match = true;
  } else if (share::schema::is_vec_ivfpq_index(type1) && share::schema::is_vec_ivfpq_index(type2)) {
    is_match = true;
  }
  return is_match;
}

int ObVectorIndexUtil::get_dropping_vec_index_invisiable_table_schema(
    const share::schema::ObTableSchema &index_table_schema,
    const uint64_t data_table_id,
    const bool is_vec_inner_drop,
    share::schema::ObSchemaGetterGuard &schema_guard,
    rootserver::ObDDLOperator &ddl_operator,
    common::ObMySQLTransaction &trans,
    common::ObIArray<share::schema::ObTableSchema> &new_aux_schemas)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchema *data_table_schema = nullptr;
  ObSEArray<const ObSimpleTableSchemaV2 *, OB_MAX_AUX_TABLE_PER_MAIN_TABLE> indexs;
  const uint64_t tenant_id = index_table_schema.get_tenant_id();
  const uint64_t index_table_id = index_table_schema.get_table_id();
  const ObString &index_name = index_table_schema.get_table_name_str();

  if (OB_UNLIKELY(OB_INVALID_ID == data_table_id
        || OB_INVALID_ID == index_table_id
        || OB_INVALID_TENANT_ID == tenant_id
        || index_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(data_table_id), K(index_table_id), K(tenant_id), K(index_name));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
    LOG_WARN("fail to get index schema with data table id", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, data table schema is nullptr", K(ret), KP(data_table_schema));
  } else {
    SMART_VAR(ObTableSchema, new_aux_schema) {
      const ObIArray<share::schema::ObAuxTableMetaInfo> &indexs = data_table_schema->get_simple_index_infos();
      const share::schema::ObTableSchema *index_id_schema = nullptr;
      const share::schema::ObTableSchema *snapshot_data_schema = nullptr;
      const share::schema::ObTableSchema *rowkey_vid_schema = nullptr;
      const share::schema::ObTableSchema *vid_rowkey_schema = nullptr;
      const share::schema::ObTableSchema *cid_vector_schema = nullptr;
      const share::schema::ObTableSchema *rowkey_cid_schema = nullptr;
      const share::schema::ObTableSchema *sq_meta_schema = nullptr;
      const share::schema::ObTableSchema *pq_centroid_schema = nullptr;
      const share::schema::ObTableSchema *pq_code_schema = nullptr;

      ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
      bool is_index = true;
      const int64_t database_id = data_table_schema->get_database_id();
      const bool is_hidden_flag = false;
      const bool is_built_in_flag = true;
      bool already_get_index_id_table = false;
      bool already_get_snapshot_data_table = false;
      bool already_get_cid_vector_table = false;
      bool already_get_rowkey_cid_table = false;
      bool already_get_sq_meta_table = false;
      bool already_get_pq_centroid_table = false;
      bool already_get_pq_code_table = false;

      for (int64_t i = 0; OB_SUCC(ret) && i < indexs.count(); ++i) {
        const share::schema::ObAuxTableMetaInfo &info = indexs.at(i);
        if (share::schema::is_vec_rowkey_vid_type(info.index_type_)) {
          if (!check_is_match_index_type(index_table_schema.get_index_type(), info.index_type_)) { // skip getting diff index type
          } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, info.table_id_, rowkey_vid_schema))) {
            LOG_WARN("fail to get vec rowkey vid table schema", K(ret), K(tenant_id), K(info));
          } else if (OB_ISNULL(rowkey_vid_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("rowkey_vid_schema is nullptr", K(ret), K(info));
          } else if (OB_FAIL(new_aux_schemas.push_back(*rowkey_vid_schema))) {
            LOG_WARN("fail to push vec rowkey vid table schema", K(ret), KPC(rowkey_vid_schema));
          }
        } else if (share::schema::is_vec_vid_rowkey_type(info.index_type_)) {
          if (!check_is_match_index_type(index_table_schema.get_index_type(), info.index_type_)) { // skip getting diff index type
          } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, info.table_id_, vid_rowkey_schema))) {
            LOG_WARN("fail to get vec vid rowkey table schema", K(ret), K(tenant_id), K(info));
          } else if (OB_ISNULL(vid_rowkey_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("vid_rowkey_schema is nullptr", K(ret), K(info));
          } else if (OB_FAIL(new_aux_schemas.push_back(*vid_rowkey_schema))) {
            LOG_WARN("fail to push vec vid rowkey table schema", K(ret), KPC(vid_rowkey_schema));
          }
        } else if (share::schema::is_vec_index_id_type(info.index_type_)) {
          // 通过索引名获取4号表
          if (!check_is_match_index_type(index_table_schema.get_index_type(), info.index_type_)) { // skip getting diff index type
          } else if (already_get_index_id_table) {
          // 主表可能存在多个4号表，但这里只取满足index_name字串的4号表,
          // 如果不判断已经拿到了，那么循环时会从主表上拿多次同样的index_schema，不符合预期，这里需要skip。
          // 下面对其他表的获取类似
          } else if (OB_FAIL(ObVecIndexBuilderUtil::get_vec_table_schema_by_name(schema_guard,
                                                                                 tenant_id,
                                                                                 database_id,
                                                                                 index_name,
                                                                                 info.index_type_,
                                                                                 &allocator,
                                                                                 index_id_schema))) {
            LOG_WARN("fail to generate vec index name", K(ret), K(info.index_type_));
          } else if (OB_ISNULL(index_id_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("index_id_schema is nullptr", K(ret), K(index_name));
          } else if (OB_FAIL(new_aux_schemas.push_back(*index_id_schema))) {
            LOG_WARN("fail to push vec table schema", K(ret), K(index_name));
          } else {
            already_get_index_id_table = true;
          }
        } else if (share::schema::is_vec_index_snapshot_data_type(info.index_type_)) {
          // 通过索引名获取5号表
          if (!check_is_match_index_type(index_table_schema.get_index_type(), info.index_type_)) { // skip getting diff index type
          } else if (already_get_snapshot_data_table) {   // skip
          } else if (OB_FAIL(ObVecIndexBuilderUtil::get_vec_table_schema_by_name(schema_guard,
                                                                                 tenant_id,
                                                                                 database_id,
                                                                                 index_name,
                                                                                 info.index_type_,
                                                                                 &allocator,
                                                                                 snapshot_data_schema))) {
            LOG_WARN("fail to generate vec index name", K(ret), K(info.index_type_));
          } else if (OB_ISNULL(snapshot_data_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("snapshot_data_schema is nullptr", K(ret), K(index_name));
          } else if (OB_FAIL(new_aux_schemas.push_back(*snapshot_data_schema))) {
            LOG_WARN("fail to push vec table schema", K(ret), K(index_name));
          } else {
            already_get_snapshot_data_table = true;
          }
        } else if (share::schema::is_vec_ivfflat_cid_vector_index(info.index_type_) ||
                   share::schema::is_vec_ivfsq8_cid_vector_index(info.index_type_)) {
            // get cid_vector
          if (!check_is_match_index_type(index_table_schema.get_index_type(), info.index_type_)) { // skip getting diff index type
          } else if (already_get_cid_vector_table) {    // skip
          } else if (OB_FAIL(ObVecIndexBuilderUtil::get_vec_table_schema_by_name(schema_guard,
                                                                                 tenant_id,
                                                                                 database_id,
                                                                                 index_name,
                                                                                 info.index_type_,
                                                                                 &allocator,
                                                                                 cid_vector_schema))) {
            LOG_WARN("fail to generate vec index name", K(ret), K(info.index_type_));
          } else if (OB_ISNULL(cid_vector_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("cid_vector_schema is nullptr", K(ret), K(index_name));
          } else if (OB_FAIL(new_aux_schemas.push_back(*cid_vector_schema))) {
            LOG_WARN("fail to push vec table schema", K(ret), K(index_name));
          } else {
            already_get_cid_vector_table = true;
          }
        } else if (share::schema::is_vec_ivfflat_rowkey_cid_index(info.index_type_) ||
                   share::schema::is_vec_ivfsq8_rowkey_cid_index(info.index_type_) ||
                   share::schema::is_vec_ivfpq_rowkey_cid_index(info.index_type_)) {
            // get rowkey_cid
          if (!check_is_match_index_type(index_table_schema.get_index_type(), info.index_type_)) { // skip getting diff index type
          } else if (already_get_rowkey_cid_table) {   // skip
          } else if (OB_FAIL(ObVecIndexBuilderUtil::get_vec_table_schema_by_name(schema_guard,
                                                                                 tenant_id,
                                                                                 database_id,
                                                                                 index_name,
                                                                                 info.index_type_,
                                                                                 &allocator,
                                                                                 rowkey_cid_schema))) {
            LOG_WARN("fail to generate vec index name", K(ret), K(info.index_type_));
          } else if (OB_ISNULL(rowkey_cid_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("rowkey_cid_schema is nullptr", K(ret), K(index_name));
          } else if (OB_FAIL(new_aux_schemas.push_back(*rowkey_cid_schema))) {
            LOG_WARN("fail to push vec table schema", K(ret), K(index_name));
          } else {
            already_get_rowkey_cid_table = true;
          }
        } else if (share::schema::is_vec_ivfsq8_meta_index(info.index_type_)) {
            // get rowkey_cid
          if (!check_is_match_index_type(index_table_schema.get_index_type(), info.index_type_)) { // skip getting diff index type
          } else if (already_get_sq_meta_table) {           // skip
          } else if (OB_FAIL(ObVecIndexBuilderUtil::get_vec_table_schema_by_name(schema_guard,
                                                                                 tenant_id,
                                                                                 database_id,
                                                                                 index_name,
                                                                                 info.index_type_,
                                                                                 &allocator,
                                                                                 sq_meta_schema))) {
            LOG_WARN("fail to generate vec index name", K(ret), K(info.index_type_));
          } else if (OB_ISNULL(sq_meta_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("sq_meta_schema is nullptr", K(ret), K(index_name));
          } else if (OB_FAIL(new_aux_schemas.push_back(*sq_meta_schema))) {
            LOG_WARN("fail to push vec table schema", K(ret), K(index_name));
          } else {
            already_get_sq_meta_table = true;
          }
        } else if (share::schema::is_vec_ivfpq_pq_centroid_index(info.index_type_)) {
            // get pg_centroid
          if (!check_is_match_index_type(index_table_schema.get_index_type(), info.index_type_)) { // skip getting diff index type
          } else if (already_get_pq_centroid_table) {     // skip
          } else if (OB_FAIL(ObVecIndexBuilderUtil::get_vec_table_schema_by_name(schema_guard,
                                                                                 tenant_id,
                                                                                 database_id,
                                                                                 index_name,
                                                                                 info.index_type_,
                                                                                 &allocator,
                                                                                 pq_centroid_schema))) {
            LOG_WARN("fail to generate vec index name", K(ret), K(info.index_type_));
          } else if (OB_ISNULL(pq_centroid_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("pq_centroid_schema is nullptr", K(ret), K(index_name));
          } else if (OB_FAIL(new_aux_schemas.push_back(*pq_centroid_schema))) {
            LOG_WARN("fail to push vec table schema", K(ret), K(index_name));
          } else {
            already_get_pq_centroid_table = true;
          }
        } else if (share::schema::is_vec_ivfpq_code_index(info.index_type_)) {
            // get pq_code_schema
          if (!check_is_match_index_type(index_table_schema.get_index_type(), info.index_type_)) { // skip getting diff index type
          } else if (already_get_pq_code_table) {          // skip
          } else if (OB_FAIL(ObVecIndexBuilderUtil::get_vec_table_schema_by_name(schema_guard,
                                                                                 tenant_id,
                                                                                 database_id,
                                                                                 index_name,
                                                                                 info.index_type_,
                                                                                 &allocator,
                                                                                 pq_code_schema))) {
            LOG_WARN("fail to generate vec index name", K(ret), K(info.index_type_));
          } else if (OB_ISNULL(pq_code_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("pq_code_schema is nullptr", K(ret), K(index_name));
          } else if (OB_FAIL(new_aux_schemas.push_back(*pq_code_schema))) {
            LOG_WARN("fail to push vec table schema", K(ret), K(index_name));
          } else {
            already_get_pq_code_table = true;
          }
        }

        if (OB_TABLE_NOT_EXIST == ret && is_vec_inner_drop) {
          ret = OB_SUCCESS;
          LOG_WARN("table is not exist, maybe index table have been drop already", K(ret));
        }
      }
    }
    LOG_INFO("get dropping vec aux table name", K(ret), K(tenant_id), K(data_table_id), K(index_table_id));
  }
  return ret;
}

int ObVectorIndexUtil::check_drop_vec_indexs_ith_valid(const ObIndexType index_type, const int64_t schema_count,
      int64_t &rowkey_vid_ith, int64_t &vid_rowkey_ith, int64_t &domain_index_ith, int64_t &index_id_ith, int64_t &snapshot_data_ith,
      int64_t &centroid_ith, int64_t &cid_vector_ith, int64_t &rowkey_cid_ith, int64_t &sq_meta_ith, int64_t &pq_centroid_ith, int64_t &pq_code_ith)
{
  int ret = OB_SUCCESS;
  if (share::schema::is_vec_hnsw_index(index_type)) {
    if (rowkey_vid_ith < 0 || rowkey_vid_ith >= schema_count ||
        vid_rowkey_ith < 0 || vid_rowkey_ith >= schema_count ||
        index_id_ith < 0 || index_id_ith >= schema_count ||
        snapshot_data_ith < 0 || snapshot_data_ith >= schema_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check drop vec hnsw index fail",
        K(ret), K(rowkey_vid_ith), K(vid_rowkey_ith), K(index_id_ith), K(snapshot_data_ith));
    }
  } else if (share::schema::is_vec_ivfflat_index(index_type)) {
    if (centroid_ith < 0 || centroid_ith >= schema_count ||
        cid_vector_ith < 0 || cid_vector_ith >= schema_count ||
        rowkey_cid_ith < 0 || rowkey_cid_ith >= schema_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check drop vec ivfflat index fail",
        K(ret), K(centroid_ith), K(cid_vector_ith), K(rowkey_cid_ith));
    }
  } else if (share::schema::is_vec_ivfsq8_index(index_type)) {
    if (centroid_ith < 0 || centroid_ith >= schema_count ||
        cid_vector_ith < 0 || cid_vector_ith >= schema_count ||
        rowkey_cid_ith < 0 || rowkey_cid_ith >= schema_count ||
        sq_meta_ith < 0 || sq_meta_ith >= schema_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check drop vec ivfsq8 index fail",
        K(ret), K(centroid_ith), K(cid_vector_ith), K(rowkey_cid_ith), K(sq_meta_ith));
    }
  } else if (share::schema::is_vec_ivfpq_index(index_type)) {
    if (centroid_ith < 0 || centroid_ith >= schema_count ||
        rowkey_cid_ith < 0 || rowkey_cid_ith >= schema_count ||
        pq_centroid_ith < 0 || pq_centroid_ith >= schema_count ||
        pq_code_ith < 0 || pq_code_ith >= schema_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check drop vec ivfpq index fail",
        K(ret), K(centroid_ith), K(pq_centroid_ith), K(rowkey_cid_ith), K(pq_code_ith));
    }
  }
  return ret;
}

int ObVectorIndexUtil::calc_residual_vector(
    ObIAllocator &alloc,
    int dim,
    const float *vector,
    const float *center_vec,
    float *&residual)
{
  int ret = OB_SUCCESS;
  residual = nullptr;
  if (OB_ISNULL(residual = reinterpret_cast<float*>(alloc.alloc(sizeof(float) * dim)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc vector", K(ret));
  } else {
    for (int64_t i = 0; i < dim; ++i) {
      residual[i] = vector[i] - center_vec[i];
    }
  }
  return ret;
}

int ObVectorIndexUtil::calc_residual_vector(
    ObIAllocator &alloc,
    int dim,
    ObIArray<float *> &centers,
    float *vector,
    ObVectorNormalizeInfo *norm_info,
    float *&residual)
{
  int ret = OB_SUCCESS;
  ObVectorClusterHelper helper;
  int64_t center_idx = 1;
  float *center_vec = nullptr;

  if (OB_FAIL(helper.get_nearest_probe_centers(
      vector,
      dim,
      centers,
      1/*nprobe*/,
      alloc,
      norm_info))) {
    LOG_WARN("failed to get nearest center", K(ret));
  } else if (OB_FAIL(helper.get_center_vector(0/*idx*/, centers, center_vec))) {
    LOG_WARN("failed to get center idx", K(ret));
  } else if (OB_FAIL(calc_residual_vector(alloc, dim, vector, center_vec, residual))) {
    LOG_WARN("fail to calc residual vector", K(ret), K(dim));
  }
  return ret;
}

int ObVectorIndexUtil::calc_location_ids(sql::ObEvalCtx &eval_ctx,
                                        sql::ObExpr *table_id_expr,
                                        sql::ObExpr *part_id_expr,
                                        ObTableID &table_id,
                                        ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDatum *res = nullptr;
  ObObjectID partition_id = OB_INVALID_ID;
  if (OB_ISNULL(table_id_expr) || table_id_expr->datum_meta_.type_ != ObUInt64Type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("calc table id expr is invalid", K(ret), KPC(table_id_expr));
  } else if (OB_FAIL(table_id_expr->eval(eval_ctx, res))) {
    LOG_WARN("calc table id expr failed", K(ret));
  } else if (OB_INVALID_ID == (table_id = res->get_uint64())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected table id", K(ret), K(table_id));
  } else if (OB_FAIL(sql::ObExprCalcPartitionBase::calc_part_and_tablet_id(part_id_expr, eval_ctx, partition_id, tablet_id))) {
    LOG_WARN("calc part and tablet id by expr failed", K(ret));
  }
  return ret;
}

// for ObExprVecIVFCenterID and ObExprVecIVFPQCenterVector
int ObVectorIndexUtil::eval_ivf_centers_common(ObIAllocator &allocator,
                                              const sql::ObExpr &expr,
                                              sql::ObEvalCtx &eval_ctx,
                                              ObIArray<float*> &centers,
                                              ObTableID &table_id,
                                              ObTabletID &tablet_id,
                                              ObVectorIndexDistAlgorithm &dis_algo,
                                              bool &contain_null,
                                              ObIArrayType *&arr)
{
  int ret = OB_SUCCESS;
  table_id = OB_INVALID_ID;
  tablet_id.reset();
  dis_algo = VIDA_MAX;
  contain_null = false;
  arr = nullptr;
  if (OB_UNLIKELY(4 != expr.arg_cnt_) || OB_ISNULL(expr.args_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(expr), KP(expr.args_));
  } else {
    ObExpr *calc_vector_expr = expr.args_[0];
    ObExpr *calc_table_id_expr = expr.args_[1];
    ObExpr *calc_part_id_expr = expr.args_[2];
    ObExpr *calc_distance_algo_expr = expr.args_[3];
    ObDatum *res = nullptr;
    if (OB_ISNULL(calc_vector_expr) || calc_vector_expr->datum_meta_.type_ != ObCollectionSQLType) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("calc vector expr is invalid", K(ret), KPC(calc_vector_expr));
    } else if (OB_FAIL(ObArrayExprUtils::get_type_vector(*(calc_vector_expr), eval_ctx, allocator, arr, contain_null))) {
      LOG_WARN("failed to get vector", K(ret), KPC(calc_vector_expr));
    } else if (OB_FAIL(ObVectorIndexUtil::calc_location_ids(eval_ctx, calc_table_id_expr, calc_part_id_expr, table_id, tablet_id))) {
      LOG_WARN("fail to calc location ids", K(ret), K(table_id), K(tablet_id), KP(calc_table_id_expr), KP(calc_part_id_expr));
    } else if (contain_null) {
      // do nothing
    } else if (OB_ISNULL(calc_distance_algo_expr) || calc_distance_algo_expr->datum_meta_.type_ != ObUInt64Type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("calc distance algo expr is invalid", K(ret), KPC(calc_distance_algo_expr));
    } else if (OB_FAIL(calc_distance_algo_expr->eval(eval_ctx, res))) {
      LOG_WARN("calc table id expr failed", K(ret));
    } else if (FALSE_IT(dis_algo = static_cast<ObVectorIndexDistAlgorithm>(res->get_uint64()))) {
    } else if (VIDA_MAX <= dis_algo) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected distance algo", K(ret), K(dis_algo));
    } else {
      ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
      ObExprVecIvfCenterIdCache *cache = get_ivf_center_id_cache_ctx(expr.expr_ctx_id_, &eval_ctx.exec_ctx_);
      if (OB_FAIL(get_ivf_aux_info(service, cache, table_id, tablet_id, allocator, centers))) {
        LOG_WARN("failed to get ivf aux info", K(ret));
      }
    }
  }
  return ret;
}

ObExprVecIvfCenterIdCache* ObVectorIndexUtil::get_ivf_center_id_cache_ctx(const uint64_t& id, sql::ObExecContext *exec_ctx)
{
  INIT_SUCC(ret);
  ObExprVecIvfCenterIdCtx* cache_ctx = NULL;
  if (ObExpr::INVALID_EXP_CTX_ID != id) {
    cache_ctx = static_cast<ObExprVecIvfCenterIdCtx*>(exec_ctx->get_expr_op_ctx(id));
    if (OB_ISNULL(cache_ctx)) {
      // if cache not exist, create one
      void *cache_ctx_buf = NULL;
      ret = exec_ctx->create_expr_op_ctx(id, sizeof(ObExprVecIvfCenterIdCtx), cache_ctx_buf);
      if (OB_SUCC(ret) && OB_NOT_NULL(cache_ctx_buf)) {
        cache_ctx = new (cache_ctx_buf) ObExprVecIvfCenterIdCtx();
      }
    }
  }
  return (cache_ctx == NULL) ? NULL : cache_ctx->get_cache();
}

void ObVectorIndexUtil::get_ivf_pq_center_id_cache_ctx(const uint64_t& id, sql::ObExecContext *exec_ctx, ObExprVecIvfCenterIdCache *&cache, ObExprVecIvfCenterIdCache *&pq_cache)
{
  INIT_SUCC(ret);
  ObExprVecIvfCenterIdCtx* cache_ctx = NULL;
  if (ObExpr::INVALID_EXP_CTX_ID != id) {
    cache_ctx = static_cast<ObExprVecIvfCenterIdCtx*>(exec_ctx->get_expr_op_ctx(id));
    if (OB_ISNULL(cache_ctx)) {
      // if cache not exist, create one
      void *cache_ctx_buf = NULL;
      ret = exec_ctx->create_expr_op_ctx(id, sizeof(ObExprVecIvfCenterIdCtx), cache_ctx_buf);
      if (OB_SUCC(ret) && OB_NOT_NULL(cache_ctx_buf)) {
        cache_ctx = new (cache_ctx_buf) ObExprVecIvfCenterIdCtx();
      }
    }
  }
  if (cache_ctx != NULL) {
    cache = cache_ctx->get_cache();
    pq_cache = cache_ctx->get_pq_cache();
  }
}

int ObVectorIndexUtil::get_ivf_aux_info(share::ObPluginVectorIndexService *service,
                                            ObExprVecIvfCenterIdCache *cache,
                                            const ObTableID &table_id,
                                            const ObTabletID &tablet_id,
                                            common::ObIAllocator &allocator,
                                            ObIArray<float*> &centers)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("service is nullptr", K(ret));
  } else {
    if (OB_ISNULL(cache)) {
      if (OB_FAIL(service->get_ivf_aux_info(table_id, tablet_id, allocator, centers))) {
        LOG_WARN("failed to get centers", K(ret));
      }
    } else {
      if (cache->hit(table_id, tablet_id)) {
        if (OB_FAIL(cache->get_centers(centers))) {
          LOG_WARN("failed to get centers from cache", K(ret));
        }
      } else {
        cache->reuse();
        if (OB_FAIL(service->get_ivf_aux_info(table_id, tablet_id, cache->get_allocator(), centers))) {
          LOG_WARN("failed to get centers", K(ret));
        } else if (OB_FAIL(cache->update_cache(table_id, tablet_id, centers))) {
          LOG_WARN("failed to update ivf center id cache", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::get_vector_index_type(
      share::schema::ObSchemaGetterGuard *schema_guard,
      const ObTableSchema &data_table_schema,
      const int64_t col_id, // index col id
      ObIndexType &index_type)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObAuxTableMetaInfo, 16>simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  index_type = ObIndexType::INDEX_TYPE_MAX;

  if (OB_ISNULL(schema_guard) || !data_table_schema.is_user_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard),  K(data_table_schema));
  } else if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count() && index_type == ObIndexType::INDEX_TYPE_MAX; ++i) {
    const ObTableSchema *index_table_schema = nullptr;
    if (OB_FAIL(schema_guard->get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_table_schema))) {
      LOG_WARN("fail to get index_table_schema", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
    } else if (!index_table_schema->is_vec_index()) {
      // skip none vector index
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && index_type == ObIndexType::INDEX_TYPE_MAX && j < index_table_schema->get_column_count(); j++) {
        const ObColumnSchemaV2 *col_schema = nullptr;
        if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
        } else if (col_schema->get_column_id() == col_id) {
          index_type = simple_index_infos.at(i).index_type_;
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::split_vector(
    ObIAllocator &alloc,
    int pq_m,
    int dim,
    float* vector,
    ObIArray<float*> &splited_arrs)
{
  int ret = OB_SUCCESS;
  int64_t start_idx = 0;
  int64_t sub_dim = dim / pq_m;
  for (int i = 0; OB_SUCC(ret) && i < pq_m; ++i) {
    float *splited_vec = nullptr;
    if (OB_ISNULL(splited_vec = static_cast<float*>(alloc.alloc(sizeof(float) * sub_dim)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SHARE_LOG(WARN, "failed to alloc vector", K(ret));
    } else {
      MEMCPY(splited_vec, vector + start_idx, sizeof(float) * sub_dim);
      if (OB_FAIL(splited_arrs.push_back(splited_vec))) {
        SHARE_LOG(WARN, "failed to push back array", K(ret), K(i));
      } else {
        start_idx += sub_dim;
      }
    }
  }
  return ret;
}

}
}
