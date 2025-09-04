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
#include "share/ob_fts_index_builder_util.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "lib/roaringbitmap/ob_rb_memory_mgr.h"
#include "lib/file/ob_string_util.h"

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
    const ObString &index_param_str, ObVectorIndexType index_type, ObVectorIndexParam &param, const bool set_default)
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
  } else if (index_type != ObVectorIndexType::VIT_SPIV_INDEX && tmp_param_strs.count() < 2) {  // at lease two params(distance, type) should be set
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector index param count", K(tmp_param_strs.count()));
  } else if (index_type == ObVectorIndexType::VIT_SPIV_INDEX && tmp_param_strs.count() != 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector index param count", K(tmp_param_strs.count()));
  } else {
    const int64_t default_m_value = 16;
    const int64_t default_ef_construction_value = 200;
    const int64_t default_ef_search_value = 64;
    const int64_t default_nlist_value = 128;
    const int64_t default_sample_per_nlist_value = 256;
    const int64_t default_nbits_value = 8;

    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_param_strs.count(); ++i) {
      ObString one_tmp_param_str = tmp_param_strs.at(i).trim();
      ObArray<ObString> one_tmp_param_strs;
      if (OB_FAIL(split_on(one_tmp_param_str, '=', one_tmp_param_strs))) {
        LOG_WARN("fail to split one param str", K(ret), K(one_tmp_param_str));
      } else if (one_tmp_param_strs.count() != 2) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid vector index one param pair count", K(one_tmp_param_strs.count()));
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
          } else if (new_param_value == "HNSW_BQ") {
            param.type_ = ObVectorIndexAlgorithmType::VIAT_HNSW_BQ;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index type", K(ret), K(new_param_value));
          }
        } else if (new_param_name == "M") { // here must be ivf_pq or hnsw index
          int64_t int_value = 0;
          if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (ObVectorIndexType::VIT_HNSW_INDEX == index_type) {
            if (int_value >= 5 && int_value <= 128) {
              param.m_ = int_value;
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support vector index m value", K(ret), K(int_value), K(new_param_value));
            }
          } else if (ObVectorIndexType::VIT_IVF_INDEX == index_type) {
            if (int_value >= 1 && int_value <= 65536) {
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
          } else if (int_value >= 1 && int_value <= 65536) {
            param.nlist_ = int_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index nlist value", K(ret), K(int_value), K(new_param_value));
          }
        } else if (new_param_name == "SAMPLE_PER_NLIST") {
          int64_t int_value = 0;
          if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (int_value >= 1 && int_value <= INT64_MAX) {
            param.sample_per_nlist_ = int_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index sample_per_nlist value", K(ret), K(int_value), K(new_param_value));
          }
        } else if (new_param_name == "EXTRA_INFO_MAX_SIZE") {
          int64_t int_value = 0;
          bool is_int = false;
          if (OB_FAIL(is_int_val(new_param_value, is_int)) || !is_int) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index extra_info_max_size value", K(ret), K(new_param_value));
          } else if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (ObVectorIndexType::VIT_HNSW_INDEX == index_type) {
            if (int_value >= 0 && int_value <= INT64_MAX) {
              param.extra_info_max_size_ = int_value;
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support vector index extra_info_max_size value", K(ret), K(int_value), K(new_param_value));
            }
          }
        } else if (new_param_name == "EXTRA_INFO_ACTUAL_SIZE") {
          int64_t int_value = 0;
          if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (ObVectorIndexType::VIT_HNSW_INDEX == index_type) {
            if (int_value >= 0 && int_value <= INT64_MAX) {
              param.extra_info_actual_size_ = int_value;
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support vector index extra_info_actual_size value", K(ret), K(int_value), K(new_param_value));
            }
          }
        } else if (new_param_name == "REFINE_TYPE") {
          if (new_param_value == "FP32") {
            param.refine_type_ = 0;
          } else if (new_param_value == "SQ8") {
            param.refine_type_ = 1;
          } else {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid vector index param", K(ret), K(new_param_name), K(new_param_value));
          }
        } else if (new_param_name == "BQ_BITS_QUERY") {
          int64_t int_value = 0;
          if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (int_value != 0 && int_value != 4 && int_value != 32) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index bq_bits_query value", K(ret), K(int_value), K(new_param_value));
          } else {
            param.bq_bits_query_ = int_value;
          }
        } else if (new_param_name == "REFINE_K") {
          int err = 0;
          char *endptr = NULL;
          double out_val = ObCharset::strntod(new_param_value.ptr(), new_param_value.length(), &endptr, &err);
          if (err != 0 || (new_param_value.ptr() + new_param_value.length()) != endptr) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("fail to cast string to double", K(ret), K(new_param_value), K(err), KP(endptr));
          } else if (out_val < 1.0 || out_val > 1e6) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index refine_k value", K(ret), K(out_val), K(new_param_value));
          } else {
            param.refine_k_ = out_val;
          }
        } else if (new_param_name == "BQ_USE_FHT") {
          if (new_param_value == "FALSE") {
            param.bq_use_fht_ = false;
          } else if (new_param_value == "TRUE") {
            param.bq_use_fht_ = true;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index bq_use_fht value", K(ret), K(new_param_name), K(new_param_value));
          }
        } else if (new_param_name == "NBITS") {
          int64_t int_value = 0;
          if (OB_FAIL(ObSchemaUtils::str_to_int(new_param_value, int_value))) {
            LOG_WARN("fail to str_to_int", K(ret), K(new_param_value));
          } else if (ObVectorIndexType::VIT_IVF_INDEX == index_type) {
            if (int_value >= 1 && int_value <= 24) {
              param.nbits_ = int_value;
            } else {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support vector index nbits value", K(ret), K(int_value), K(new_param_value));
            }
          }
        } else {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid vector index param name", K(ret), K(new_param_name));
        }
        if (index_type == ObVectorIndexType::VIT_SPIV_INDEX) {
          param.type_ = ObVectorIndexAlgorithmType::VIAT_SPIV;
        }
      }
    }

    if (OB_SUCC(ret) && set_default) {  // if vector param is not set, use default
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
        if (param.extra_info_actual_size_ > 0 && param.type_ == ObVectorIndexAlgorithmType::VIAT_HNSW) {
          param.type_ = ObVectorIndexAlgorithmType::VIAT_HGRAPH;
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
        if (param.nbits_ == 0) {
          param.nbits_ = default_nbits_value;
        }
      } else if (index_type == ObVectorIndexType::VIT_SPIV_INDEX) {
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

int ObVectorIndexUtil::is_int_val(const ObString &str, bool &is_int)
{
  int ret = OB_SUCCESS;
  is_int = false;
  char buf[OB_MAX_BIT_LENGTH];
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(str));
  } else {
    int n = snprintf(buf, OB_MAX_BIT_LENGTH, "%.*s", str.length(), str.ptr());
    if (n < 0 || n >= OB_MAX_BIT_LENGTH) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("id_buf is not long enough", K(ret), K(n), LITERAL_K(OB_MAX_BIT_LENGTH));
    } else {
      is_int = ::obsys::ObStringUtil::is_int(buf);
    }
  }
  return ret;
}

int ObVectorIndexParam::print_to_string(char *buf, int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  #define PRINT_PARAM(fmt, val)                                        \
    if (OB_SUCC(ret)) {                                                \
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, fmt, val))) {     \
        LOG_WARN("fail to print param", K(ret), K(fmt), K(val));       \
      }                                                                \
    }
  PRINT_PARAM("type=%d,", type_);
  PRINT_PARAM("lib=%d,", lib_);
  PRINT_PARAM("dist_algorithm=%d,", dist_algorithm_);
  PRINT_PARAM("dim=%ld,", dim_);
  PRINT_PARAM("m=%ld,", m_);
  PRINT_PARAM("ef_construction=%ld,", ef_construction_);
  PRINT_PARAM("ef_search=%ld,", ef_search_);
  PRINT_PARAM("nlist=%ld,", nlist_);
  PRINT_PARAM("sample_per_nlist=%ld,", sample_per_nlist_);
  PRINT_PARAM("extra_info_max_size=%ld,", extra_info_max_size_);
  PRINT_PARAM("extra_info_actual_size=%ld,", extra_info_actual_size_);
  PRINT_PARAM("refine_type=%d,", static_cast<int>(refine_type_));
  PRINT_PARAM("bq_bits_query=%d,", static_cast<int>(bq_bits_query_));
  PRINT_PARAM("refine_k=%f,", refine_k_);
  PRINT_PARAM("bq_use_fht=%d,", static_cast<int>(bq_use_fht_));
  PRINT_PARAM("sync_interval_type=%d,", static_cast<int>(sync_interval_type_));
  PRINT_PARAM("sync_interval_value=%ld,", sync_interval_value_);
  PRINT_PARAM("endpoint=%s,", endpoint_);
  PRINT_PARAM("nbits=%ld", nbits_);
  #undef PRINT_PARAM
  return ret;
}

int ObVectorIndexParam::build_search_param(const ObVectorIndexParam &index_param,
                                           const ObVectorIndexQueryParam &query_param,
                                           ObVectorIndexParam &search_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(search_param.assign(index_param))) {
    LOG_WARN("fail to parser params from string", K(ret), K(index_param));
  } else {
    if (query_param.is_set_ef_search_) {
      search_param.ef_search_ = query_param.ef_search_;
    }
    if (query_param.is_set_refine_k_) {
      if (ObVectorIndexAlgorithmType::VIAT_HNSW_BQ != index_param.type_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("refine_k is not support parameter for current index", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "refine_k parameter for current index is");
      } else {
        search_param.refine_k_ = query_param.refine_k_;
      }
    }
    LOG_TRACE("vector param", K(index_param), K(query_param), K(search_param));
  }
  return ret;
}

int ObVectorIndexUtil::resolve_query_param(
    const ParseNode *param_list_node,
    ObVectorIndexQueryParam &param)
{
  int ret = OB_SUCCESS;
  const ParseNode *param_node = nullptr;
  for (int32_t i = 0; OB_SUCC(ret) && i < param_list_node->num_child_; i+=2) {
    if (i + 1 >= param_list_node->num_child_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("num_child is incorrect", K(ret), K(i), K(param_node->num_child_), K(param_node->type_));
    } else {
      ObString param_name;
      const ParseNode *key_node = param_list_node->children_[i];
      const ParseNode *value_node = param_list_node->children_[i + 1];
      param_name.assign_ptr(key_node->str_value_, static_cast<int32_t>(key_node->str_len_));
      if (param_name.case_compare("EF_SEARCH") == 0) {
        if (param.is_set_ef_search_) {
          ret = OB_ERR_PARAM_DUPLICATE;
          LOG_WARN("duplicate ef_search param", K(ret), K(i));
        } else if (value_node->type_ != T_INT && value_node->type_ != T_NUMBER) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid query param", K(ret), K(i), K(param_name), K(value_node->type_));
        } else if (! (value_node->value_ >= 1 && value_node->value_ <= 1000)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid query param", K(ret), K(i), K(param_name), K(value_node->type_), K(value_node->value_));
        } else {
          param.ef_search_ = value_node->value_;
          param.is_set_ef_search_ = 1;
        }
      } else if (param_name.case_compare("REFINE_K") == 0) {
        int err = 0;
        char *endptr = NULL;
        ObString value_str(static_cast<int32_t>(value_node->str_len_), value_node->str_value_);
        double out_val = 0;
        if (param.is_set_refine_k_) {
          ret = OB_ERR_PARAM_DUPLICATE;
          LOG_WARN("duplicate refine_k param", K(ret), K(i));
        } else if (OB_FALSE_IT(out_val = ObCharset::strntod(value_str.ptr(), value_str.length(), &endptr, &err))) {
        } else if (err != 0 || (value_str.ptr() + value_str.length()) != endptr) {
          ret = OB_DATA_OUT_OF_RANGE;
          LOG_WARN("fail to cast string to double", K(ret), K(value_str), K(err), KP(endptr));
        } else if (out_val < 1.0 || out_val > 1000) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid vector index refine_k value", K(ret), K(out_val), K(value_str));
        } else {
          param.refine_k_ = out_val;
          param.is_set_refine_k_ = 1;
        }
      } else {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid query param", K(ret), K(i), K(param_name));
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::filter_index_param(const ObString &index_param_str, const char *to_filter, char *filtered_param_str, int32_t &res_len)
{
  int ret = OB_SUCCESS;
  ObString tmp_param_str = index_param_str;
  ObArray<ObString> tmp_param_strs;
  if (tmp_param_str.empty() ||OB_ISNULL(filtered_param_str) ||OB_ISNULL(to_filter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected empty", K(ret), K(tmp_param_str), K(filtered_param_str), K(to_filter));
  } else if (OB_FAIL(split_on(tmp_param_str, ',', tmp_param_strs))) {
    LOG_WARN("fail to split func expr", K(ret), K(tmp_param_str));
  } else if (tmp_param_strs.count() < 2) {  // at lease two params(distance, type) should be set
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector index param count", K(tmp_param_strs.count()));
  } else {
    bool first_item = true;
    int64_t print_pos = 0;
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
        // filter EXTRA_INFO_ACTUAL_SIZE
        if (new_param_name != to_filter) {
          if (OB_FAIL(databuff_printf(filtered_param_str, OB_MAX_TABLE_NAME_LENGTH, print_pos,
                                      first_item ? "%.*s=%.*s" : ", %.*s=%.*s", new_param_name.length(),
                                      new_param_name.ptr(), new_param_value.length(), new_param_value.ptr()))) {
            LOG_WARN("fail to databuff printf", K(ret), K(print_pos), K(new_param_name), K(new_param_value));
          } else {
            first_item = false;
          }
        } else {
          LOG_INFO("do not print: ", K(new_param_name), K(new_param_value));
        }
      }
    }
    res_len = print_pos;
  }
  return ret;
}

int ObVectorIndexUtil::print_index_param(const ObTableSchema &table_schema, char *buf, const int64_t &buf_len,
                                         int64_t &pos)
{
  int ret = OB_SUCCESS;
  bool has_extra_param = false;
  const ObString &index_param_str = table_schema.get_index_params();
  if (share::schema::is_vec_hnsw_index(table_schema.get_index_type())) {
    const char *to_filter = "EXTRA_INFO_ACTUAL_SIZE";
    const char *position = strstr(index_param_str.ptr(), to_filter);
    if (position != NULL) {
      has_extra_param = true;
      char print_params_str[OB_MAX_TABLE_NAME_LENGTH];
      int32_t len = 0;
      if (OB_FAIL(filter_index_param(index_param_str, to_filter, print_params_str, len))) {
        LOG_WARN("fail to filter index param", K(ret), K(index_param_str));
      } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "WITH (%.*s) ", len, print_params_str))) {
        LOG_WARN("print WITH vector index param failed", K(ret), K(print_params_str));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (index_param_str.empty() || has_extra_param) {
    // skip
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "WITH (%.*s) ", index_param_str.length(),
                                     index_param_str.ptr()))) {
    LOG_WARN("print WITH vector index param failed", K(ret), K(index_param_str));
  }
  return ret;
}

int ObVectorIndexUtil::construct_rebuild_index_param(
  const ObTableSchema &data_table_schema, const ObString &old_index_params, ObString &new_index_params, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam old_vec_param;
  ObVectorIndexParam new_vec_param;
  ObString old_index_param_str;
  const bool set_default = false;
  if (old_index_params.empty() || new_index_params.empty() || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params", K(ret), K(old_index_params), K(new_index_params), KP(allocator));
  } else if (OB_FAIL(ob_simple_low_to_up(*allocator, old_index_params, old_index_param_str))) {
    LOG_WARN("string low to up failed", K(ret), K(old_index_params));
  } else if (OB_FAIL(ob_simple_low_to_up(*allocator, new_index_params, new_index_params))) {
    LOG_WARN("string low to up failed", K(ret), K(new_index_params));
  } else if (OB_FAIL(parser_params_from_string(old_index_param_str, ObVectorIndexType::VIT_HNSW_INDEX, old_vec_param, set_default))) {
    LOG_WARN("fail to parse old index parsm", K(ret), K(old_index_param_str));
  } else if (OB_FAIL(parser_params_from_string(new_index_params, ObVectorIndexType::VIT_HNSW_INDEX, new_vec_param, set_default))) {
    LOG_WARN("fail to parse new index parsm", K(ret), K(new_index_params));
  } else {
    bool new_distance_is_set = false;
    bool new_type_is_set = false;
    char not_set_params_str[OB_MAX_TABLE_NAME_LENGTH];
    int64_t pos = 0;
    // set distance
    if (new_vec_param.dist_algorithm_ != ObVectorIndexDistAlgorithm::VIDA_MAX) {
      // distance is reset, check new set is same as old, not support rebuild to new distance algorithm
      if (new_vec_param.dist_algorithm_ != old_vec_param.dist_algorithm_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("distance must be same in rebuild now", K(ret), K(new_vec_param), K(old_vec_param));
      } else {
        new_distance_is_set = true;
      }
    } else {
      ObString tmp_distance;
      if (old_vec_param.dist_algorithm_ == ObVectorIndexDistAlgorithm::VIDA_L2) {
        tmp_distance = ObString("L2");
      } else if (old_vec_param.dist_algorithm_ == ObVectorIndexDistAlgorithm::VIDA_IP) {
        tmp_distance = ObString("IP");
      } else if (old_vec_param.dist_algorithm_ == ObVectorIndexDistAlgorithm::VIDA_COS) {
        tmp_distance = ObString("COSINE");
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected old vec param distance algorithm", K(ret), K(old_vec_param.dist_algorithm_));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
          ", DISTANCE=%.*s", tmp_distance.length(), tmp_distance.ptr()))) {
        LOG_WARN("fail to databuff printf", K(ret), K(pos), K(tmp_distance));
      }
    }
    // set lib
    if (OB_FAIL(ret)) {
    } else if (new_vec_param.lib_ != ObVectorIndexAlgorithmLib::VIAL_MAX) {
      // lib is reset, check new set is same as old, not support rebuild to new lib
      if (new_vec_param.lib_ != old_vec_param.lib_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("lib must be same in rebuild now", K(ret), K(new_vec_param), K(old_vec_param));
      }
    } else {
      ObString tmp_lib;
      if (old_vec_param.lib_ == ObVectorIndexAlgorithmLib::VIAL_VSAG) {
        tmp_lib = ObString("VSAG");
      } else if (old_vec_param.lib_ == ObVectorIndexAlgorithmLib::VIAL_OB) {
        tmp_lib = ObString("OB");
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected old vec param lib", K(ret), K(old_vec_param.lib_));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
          ", LIB=%.*s", tmp_lib.length(), tmp_lib.ptr()))) {
        LOG_WARN("fail to databuff printf", K(ret), K(pos), K(tmp_lib));
      }
    }
    // set type
    if (OB_FAIL(ret)) {
    } else if (new_vec_param.type_ != ObVectorIndexAlgorithmType::VIAT_MAX) {
      // type is reset, check new set is same as old, only support rebuild from hnsw <==> hnsw_sq
      if ((new_vec_param.type_ != ObVectorIndexAlgorithmType::VIAT_HNSW && new_vec_param.type_ != ObVectorIndexAlgorithmType::VIAT_HNSW_SQ) ||
          (old_vec_param.type_ != ObVectorIndexAlgorithmType::VIAT_HNSW && old_vec_param.type_ != ObVectorIndexAlgorithmType::VIAT_HNSW_SQ)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("it must be rebuild from hnsw <==> hnsw_sq now", K(ret), K(new_vec_param), K(old_vec_param));
      } else {
        new_type_is_set = true;
      }
    } else {
      ObString tmp_type;
      if (old_vec_param.type_ == ObVectorIndexAlgorithmType::VIAT_HNSW) {
        tmp_type = ObString("HNSW");
      } else if (old_vec_param.type_ == ObVectorIndexAlgorithmType::VIAT_HNSW_SQ) {
        tmp_type = ObString("HNSW_SQ");
      } else if (old_vec_param.type_ == ObVectorIndexAlgorithmType::VIAT_IVF_FLAT) {
        tmp_type = ObString("IVF_FLAT");
      } else if (old_vec_param.type_ == ObVectorIndexAlgorithmType::VIAT_IVF_SQ8) {
        tmp_type = ObString("IVF_SQ8");
      } else if (old_vec_param.type_ == ObVectorIndexAlgorithmType::VIAT_IVF_PQ) {
        tmp_type = ObString("IVF_PQ");
      } else if (old_vec_param.type_ == ObVectorIndexAlgorithmType::VIAT_HNSW_BQ) {
        tmp_type = ObString("HNSW_BQ");
      } else if (old_vec_param.type_ == ObVectorIndexAlgorithmType::VIAT_HGRAPH) {
        tmp_type = ObString("HGRAPH");
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected old vec param type", K(ret), K(old_vec_param.type_));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos, ", TYPE=%.*s", tmp_type.length(), tmp_type.ptr()))) {
        LOG_WARN("fail to databuff printf", K(ret), K(pos), K(tmp_type));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (new_vec_param.m_ != 0) {
      // m is reset
    } else if (OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos, ", M=%ld", old_vec_param.m_))) {
      LOG_WARN("fail to databuff printf", K(ret), K(pos), K(old_vec_param.m_));
    }

    if (OB_FAIL(ret)) {
    } else if (new_vec_param.ef_search_ != 0) {
      // ef_search is reset
    } else if (OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos, ", EF_SEARCH=%ld", old_vec_param.ef_search_))) {
      LOG_WARN("fail to databuff printf", K(ret), K(pos), K(old_vec_param.ef_search_));
    }

    if (OB_FAIL(ret)) {
    } else if (new_vec_param.ef_construction_ != 0) {
      // ef_construction is reset
    } else if (OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos, ", EF_CONSTRUCTION=%ld", old_vec_param.ef_construction_))) {
      LOG_WARN("fail to databuff printf", K(ret), K(pos), K(old_vec_param.ef_construction_));
    }
    // verify extra_info_max_size and extra_info_actual_size
    if (OB_FAIL(ret)) {
    } else if (new_vec_param.extra_info_actual_size_ != 0) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unexpected setting of vector index param, extra_info_actual_size can not be set", K(ret),
               K(new_vec_param), K(old_vec_param));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "not supproted change extra_info_actual_size param");
    } else if (new_vec_param.extra_info_max_size_ != old_vec_param.extra_info_max_size_) {
      // extra_info_max_size changed
      if (new_vec_param.extra_info_max_size_ == 0) {
        // skip, do nothing
        LOG_DEBUG("extra_info_max_size set to 0, skip", K(new_vec_param), K(old_vec_param));
      } else {
        int64_t extra_info_actual_size = 0;
        if (data_table_schema.get_index_type() != ObIndexType::INDEX_TYPE_IS_NOT) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("must not index table", K(ret), K(data_table_schema));
        } else if (OB_FAIL(check_extra_info_size(data_table_schema, nullptr, true, new_vec_param.extra_info_max_size_,
                                                 extra_info_actual_size))) {
          LOG_WARN("fail to check extra info size", K(ret), K(new_vec_param), K(old_vec_param), K(data_table_schema));
        } else if (OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
                                           ", EXTRA_INFO_ACTUAL_SIZE=%ld", extra_info_actual_size))) {
          LOG_WARN("fail to printf databuff", K(ret));
        }
      }
    } else {
      // extra_info_max_size not change
      if (old_vec_param.extra_info_actual_size_ != 0) {
        if (OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos, ", EXTRA_INFO_ACTUAL_SIZE=%ld",
                                    old_vec_param.extra_info_actual_size_))) {
          LOG_WARN("fail to printf databuff", K(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (new_distance_is_set && new_type_is_set) {
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unexpected setting of vector index param, distance or type has not been set",
        K(ret), K(new_distance_is_set), K(new_type_is_set));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "the vector index params of distance or type not set is");
    }

    if (OB_SUCC(ret)) {
      char *buf = nullptr;
      const int64_t alloc_len = new_index_params.length() + pos;
      if (OB_ISNULL(buf = (static_cast<char *>(allocator->alloc(alloc_len))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory for vector index param", K(ret), K(alloc_len));
      } else {
        MEMCPY(buf, new_index_params.ptr(), new_index_params.length());
        MEMCPY(buf + new_index_params.length(), not_set_params_str, pos);
        new_index_params.assign_ptr(buf, alloc_len);
        LOG_DEBUG("vector index params", K(new_index_params));
      }
    }
    LOG_DEBUG("construct_rebuild_index_param", K(new_index_params));
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
          } else if (index_schema->is_vec_spiv_index()) {
            index_type = ObVectorIndexType::VIT_SPIV_INDEX;
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

int ObVectorIndexUtil::check_ivf_lob_inrow_threshold(
    const int64_t tenant_id,
    const ObString &database_name,
    const ObString &table_name,
    ObSchemaGetterGuard &schema_guard,
    const int64_t lob_inrow_threshold)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  int64_t max_vec_len_with_ivf = 0;
  const ObTableSchema *data_table_schema = NULL;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                                            database_name,
                                            table_name,
                                            false/*is_index*/,
                                            data_table_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(tenant_id), K(database_name), K(table_name));
  } else if (NULL == data_table_schema) {
    ret = OB_ERR_TABLE_EXIST;
    LOG_WARN("table not exist", K(ret));
  } else if (OB_FAIL(data_table_schema->get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    const ObTableSchema *index_table_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_table_schema))) {
      LOG_WARN("fail to get index_table_schema", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
    } else if (!index_table_schema->is_vec_ivf_centroid_index()) {
      // skip none ivf centroid vector index
    } else {
      // get dim
      int64_t vector_dim = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < index_table_schema->get_column_count(); j++) {
        const ObColumnSchemaV2 *col_schema = nullptr;
        if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
        } else if (!col_schema->is_vec_ivf_center_vector_column()) {
          // skip none ivf centroid vector column
        } else {
          if (OB_FAIL(ObVectorIndexUtil::get_vector_dim_from_extend_type_info(col_schema->get_extended_type_info(), vector_dim))) {
            LOG_WARN("fail to get vector dim", K(ret), K(col_schema));
          } else {
            int cur_len = sizeof(float) * vector_dim;
            max_vec_len_with_ivf = max_vec_len_with_ivf > cur_len ? max_vec_len_with_ivf : cur_len;
          }
        }
      }

      // get sub_dim if ivf_pq
      if (OB_SUCC(ret)) {
        ObString index_param_str = index_table_schema->get_index_params();
        ObVectorIndexParam index_param;
        if (index_param_str.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid index param", K(ret), K(index_param_str));
        } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(index_param_str,
                                                                ObVectorIndexType::VIT_IVF_INDEX, index_param))) {
          LOG_WARN("failed to parser params from string", K(ret), K(index_param_str));
        } else if (index_param.type_ == VIAT_IVF_PQ) {
          // 4(offset) * m + 16(pq centroid id) * m + 1(bitmap) * m = 21 * m
          int64_t pq_cent_ids_len = (OB_DOC_ID_COLUMN_BYTE_LENGTH/*pq center id length*/ +
                                       sizeof(uint8_t)/*array bitmap length*/ +
                                       sizeof(uint32_t)/*array offset length*/) *
                                       index_param.m_;
          max_vec_len_with_ivf = max_vec_len_with_ivf > pq_cent_ids_len ? pq_cent_ids_len : pq_cent_ids_len;
        }
      }
    }
  }

  if (OB_SUCC(ret) && lob_inrow_threshold < max_vec_len_with_ivf) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(ERROR, "invalid inrow threshold", K(ret), K(lob_inrow_threshold));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT,
      "lob inrow threshold, should be greater than the vector length of the vector column with IVF index");
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

int ObVectorIndexUtil::check_has_extra_info(const ObTableSchema &data_table_schema, ObSchemaGetterGuard &schema_guard,
                                            bool &has_extra_info)
{
  int ret = OB_SUCCESS;

  has_extra_info = false;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count() && !has_extra_info; ++i) {
      ObVectorIndexParam param;
      if (schema::is_vec_delta_buffer_type(simple_index_infos.at(i).index_type_)) {
        const ObTableSchema *index_table_schema = nullptr;
        if (OB_FAIL(schema_guard.get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_table_schema))) {
          LOG_WARN("fail to get index_table_schema", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
        } else if (OB_ISNULL(index_table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
        } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(index_table_schema->get_index_params(),
                                                                        ObVectorIndexType::VIT_HNSW_INDEX, param))) {
          LOG_WARN("fail to parser params from string", K(ret), K(index_table_schema->get_index_params()));
        } else if (param.extra_info_actual_size_ > 0) {
          has_extra_info = true;
        }
      }
    }
  }

  return ret;
}

int ObVectorIndexUtil::determine_vid_type(const ObTableSchema &table_schema, ObDocIDType &vid_type)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;
  uint64_t vid_col_id = OB_INVALID_ID;
  static constexpr bool ENABLE_VID_OPT = true;
  // 1. check sys tenant data version
  if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) {
    LOG_WARN("fail to get sys tenant data version", KR(ret), K(data_version));
  } else if (data_version < MIN_DATA_VERSION_FOR_DOC_ID_OPT) {
    vid_type = ObDocIDType::TABLET_SEQUENCE;
  } else if (!table_schema.is_table_with_hidden_pk_column()) {
    vid_type = ObDocIDType::TABLET_SEQUENCE;
  } else if (OB_FAIL(table_schema.get_vec_index_vid_col_id(vid_col_id, false))) {
    if (OB_ERR_INDEX_KEY_NOT_FOUND == ret) {
      ret = OB_SUCCESS;
      vid_type = ENABLE_VID_OPT ? ObDocIDType::HIDDEN_INC_PK : ObDocIDType::TABLET_SEQUENCE;
    } else {
      LOG_WARN("Failed to check docid in schema", K(ret));
    }
  } else {
    // exists
    vid_type = ObDocIDType::TABLET_SEQUENCE;
  }
  return ret;
}

int ObVectorIndexUtil::check_column_has_vector_index(
    const ObTableSchema &data_table_schema, ObSchemaGetterGuard &schema_guard, const int64_t col_id, bool &is_column_has_vector_index, ObIndexType& index_type)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
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

int ObVectorIndexUtil::get_extra_info_column_id(
  const ObTableSchema &data_table_schema, const ObTableSchema &index_table_schema, ObSEArray<uint64_t, 4> &extra_col_ids)
{
  int ret = OB_SUCCESS;

  if (!index_table_schema.is_vec_delta_buffer_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index table is not delta_buffer", K(ret), K(index_table_schema));
  } else {
    ObArray<uint64_t> part_key_ids;
    ObTableSchema::const_column_iterator tmp_begin = index_table_schema.column_begin();
    ObTableSchema::const_column_iterator tmp_end = index_table_schema.column_end();
    for (; OB_SUCC(ret) && tmp_begin != tmp_end; tmp_begin++) {
      ObColumnSchemaV2 *col_schema = (*tmp_begin);
      const ObColumnSchemaV2 *ori_col_schema = data_table_schema.get_column_schema(col_schema->get_column_id());
      if (OB_NOT_NULL(ori_col_schema)) {
        if (ori_col_schema->is_rowkey_column()) {
          if (OB_FAIL(extra_col_ids.push_back(col_schema->get_column_id()))) {
            LOG_WARN("failed to push back column id", K(ret), K(col_schema->get_column_id()), K(extra_col_ids));
          }
        } else if (ori_col_schema->is_tbl_part_key_column()) {
          if (OB_FAIL(part_key_ids.push_back(col_schema->get_column_id()))) {
            LOG_WARN("failed to push back column id", K(ret), K(col_schema->get_column_id()), K(part_key_ids));
          }
        }
      }
    }
    if (OB_SUCC(ret) && extra_col_ids.count() > 0 && part_key_ids.count() > 0) {
      for (int i = 0; OB_SUCC(ret) && i < part_key_ids.count(); ++i) {
        if (OB_FAIL(extra_col_ids.push_back(part_key_ids.at(i)))) {
          LOG_WARN("failed to push back column id", K(ret), K(part_key_ids.at(i)), K(extra_col_ids));
        }
      }
      if (OB_SUCC(ret)) {
        lib::ob_sort(extra_col_ids.begin(), extra_col_ids.end(), column_id_asc_compare);
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
    if (0 == spilt_str.compare("ARRAY") || 0 == spilt_str.compare("MAP")) {
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
  目前只支持单列向量索引。
 */
 int ObVectorIndexUtil::is_sparse_vec_col(const ObIArray<ObString> &extend_type_info, bool &is_sparse_vec_col)
 {
  int ret = OB_SUCCESS;
  is_sparse_vec_col = false;

  if (extend_type_info.count() != 1) {
    // sparse vector index columns currently only support single column vector indexes.
    // When building the vector column of the auxiliary table, only one column of extend_type_info is assigned.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unexpected extend type info, current only support one column vector index",
      K(ret), K(extend_type_info));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "create vector index on more than one column is");
  } else {
    ObString str = extend_type_info.at(0);
    is_sparse_vec_col = (0 == str.compare("SPARSEVECTOR"));
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

int ObVectorIndexUtil::check_rowkey_cid_table_readable(
    share::schema::ObSchemaGetterGuard *schema_guard,
    const ObTableSchema &data_table_schema,
    const uint64_t column_id,
    uint64_t &tid,
    const bool allow_unavailable)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16>simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  tid = OB_INVALID_ID;

  if (OB_ISNULL(schema_guard) || !data_table_schema.is_user_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard), K(data_table_schema));
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
      } else if (!allow_unavailable && !index_table_schema->can_read_index()) {
      } else if (!index_table_schema->is_vec_ivfflat_rowkey_cid_index() && !index_table_schema->is_vec_ivfpq_rowkey_cid_index() && !index_table_schema->is_vec_ivfsq8_rowkey_cid_index()) {
        // skip not spec index type
      } else {
        int64_t last_shcema_version = OB_INVALID_ID;
        for (int64_t j = 0; OB_SUCC(ret) && j < index_table_schema->get_column_count(); j++) {
          const ObColumnSchemaV2 *col_schema = nullptr;
          if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
          } else if (col_schema->get_column_id() == column_id) {
            tid = simple_index_infos.at(i).table_id_;
          }
        }
      }
    }
  }
  return ret;
}

// when rowkey-vid table is readable, can get the table id
int ObVectorIndexUtil::check_rowkey_tid_table_readable(
    share::schema::ObSchemaGetterGuard *schema_guard,
    const ObTableSchema &data_table_schema,
    uint64_t &tid,
    const bool allow_unavailable)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  tid = OB_INVALID_ID;

  if (OB_ISNULL(schema_guard) || !data_table_schema.is_user_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard), K(data_table_schema));
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
      } else if (!allow_unavailable && !index_table_schema->can_read_index()) {
      } else if (!index_table_schema->is_vec_rowkey_vid_type()) {
        // skip not spec index type
      } else {
        tid = simple_index_infos.at(i).table_id_;
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::get_right_index_tid_in_rebuild(
    share::schema::ObSchemaGetterGuard *schema_guard,
    const ObTableSchema &data_table_schema,
    const ObIndexType index_type,
    const int64_t base_col_id,
    const ObColumnSchemaV2 *column_schema,
    uint64_t &tid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16>simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  tid = OB_INVALID_ID;

  ObIndexType next_index_type = index_type;
  const ObTableSchema *next_index_schema = nullptr;
  int64_t other_shcema_version = OB_INVALID_ID;
  next_index_type = INDEX_TYPE_VEC_IVFSQ8_META_LOCAL == next_index_type ? INDEX_TYPE_VEC_IVFSQ8_CID_VECTOR_LOCAL : next_index_type;
  next_index_type = column_schema->is_vec_ivf_pq_center_ids_column() ? INDEX_TYPE_VEC_IVFPQ_CODE_LOCAL : next_index_type;
  next_index_type = column_schema->is_vec_ivf_center_vector_column() && column_schema->get_column_name_str().prefix_match(OB_VEC_IVF_PQ_CENTER_VECTOR_COLUMN_NAME_PREFIX) ? INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL : next_index_type;
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
      } else if (index_table_schema->get_index_type() != next_index_type) {
        // skip not spec index type
      } else if (index_table_schema->is_vec_rowkey_vid_type() || index_table_schema->is_vec_vid_rowkey_type()) {
        // rowkey_vid and vid_rowkey is shared, only one, just return
        tid = simple_index_infos.at(i).table_id_;
      } else if (index_table_schema->is_doc_id_rowkey() || index_table_schema->is_rowkey_doc_id()) {
        tid = simple_index_infos.at(i).table_id_;
      } else if (is_local_vec_spiv_index(index_type)) {
        tid = simple_index_infos.at(i).table_id_;
      }  else if (is_local_vec_ivf_index(index_type)) {
        bool has_same_col_id = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < index_table_schema->get_column_count(); j++) {
          const ObColumnSchemaV2 *col_schema = nullptr;
          const ObColumnSchemaV2 *origin_col_schema = nullptr;
          if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
          } else if (col_schema->get_column_id() == column_schema->get_column_id()) {
            tid = simple_index_infos.at(i).table_id_;
            next_index_schema = index_table_schema;
          } else if (OB_ISNULL(origin_col_schema = data_table_schema.get_column_schema(col_schema->get_column_id()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(col_schema->get_column_id()), K(data_table_schema));
          } else if ((origin_col_schema->is_vec_ivf_data_vector_column() && column_schema->is_vec_ivf_data_vector_column())
              || (origin_col_schema->is_vec_ivf_pq_center_ids_column() && column_schema->is_vec_ivf_pq_center_ids_column())
              || (origin_col_schema->is_vec_ivf_center_vector_column() && column_schema->is_vec_ivf_center_vector_column()
              && origin_col_schema->get_column_name_str().prefix_match(OB_VEC_IVF_PQ_CENTER_VECTOR_COLUMN_NAME_PREFIX)
              && column_schema->get_column_name_str().prefix_match(OB_VEC_IVF_PQ_CENTER_VECTOR_COLUMN_NAME_PREFIX))) {
            if (OB_FAIL(has_same_cascaded_col_id(data_table_schema, *origin_col_schema, base_col_id, has_same_col_id))) {
              LOG_WARN("fail to do has_same_cascaded_col_id", K(ret), K(base_col_id));
            } else if (has_same_col_id) {
              other_shcema_version = index_table_schema->get_schema_version();
            }
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
          } else if (OB_FAIL(has_same_cascaded_col_id(data_table_schema, *col_schema, base_col_id, has_same_col_id))) {
            LOG_WARN("fail to do has_same_cascaded_col_id", K(ret), K(base_col_id));
          } else if (has_same_col_id) {
            tid = simple_index_infos.at(i).table_id_;
          }
        }
      }
    } // end for.
  }

  if (OB_SUCC(ret) && index_type != next_index_type && OB_NOT_NULL(next_index_schema)) {
    const bool is_new_index = next_index_schema->get_schema_version() > other_shcema_version && OB_INVALID_ID != other_shcema_version;
    const ObTableSchema *old_index = nullptr;
    const ObTableSchema *new_index = nullptr;
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
      } else {
        bool has_same_col_id = false;
        for (int64_t k = 0; OB_SUCC(ret) && k < index_table_schema->get_column_count(); k++) {
          const ObColumnSchemaV2 *cid_col_schema = nullptr;
          const ObColumnSchemaV2 *origin_col_schema = nullptr;
          if (OB_ISNULL(cid_col_schema = index_table_schema->get_column_schema_by_idx(k))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(k), KPC(index_table_schema));
          } else if (OB_ISNULL(origin_col_schema = data_table_schema.get_column_schema(cid_col_schema->get_column_id()))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(cid_col_schema->get_column_id()), K(data_table_schema));
          } else if (!origin_col_schema->is_vec_ivf_meta_id_column() && !origin_col_schema->is_vec_ivf_pq_center_id_column() && !origin_col_schema->is_vec_ivf_center_id_column()) {
          } else if (OB_FAIL(has_same_cascaded_col_id(data_table_schema, *origin_col_schema, base_col_id, has_same_col_id))) {
            LOG_WARN("fail to do has_same_cascaded_col_id", K(ret), K(base_col_id));
          } else if (has_same_col_id) {
            if (OB_ISNULL(old_index)) {
              old_index = index_table_schema;
            } else if(old_index->get_schema_version() < index_table_schema->get_schema_version()) {
              new_index = index_table_schema;
            } else {
              new_index = old_index;
              old_index = index_table_schema;
            }
          } else {
            ObArray<uint64_t> cascaded_column_ids;
            // get column_schema from data table using generate column id
            const ObColumnSchemaV2 *ori_col_schema = data_table_schema.get_column_schema(cid_col_schema->get_column_id());
            if (OB_ISNULL(ori_col_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected table column", K(ret), K(cid_col_schema->get_column_id()), K(data_table_schema));
            } else if (OB_FAIL(ori_col_schema->get_cascaded_column_ids(cascaded_column_ids))) {
              LOG_WARN("failed to get cascaded column ids", K(ret));
            }
          }
        }
      }
    } // end for.
    if (is_new_index) {
      if (OB_NOT_NULL(new_index)) {
        tid = new_index->get_table_id();
      }
    } else if (OB_NOT_NULL(old_index)) {
      tid = old_index->get_table_id();
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

  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
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
      } else if (index_table_schema->is_doc_id_rowkey() || index_table_schema->is_rowkey_doc_id()) {
        tid = simple_index_infos.at(i).table_id_;
      } else if (is_local_vec_spiv_index(index_type)) {
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

// Notice: only used in ivf dml
int ObVectorIndexUtil::get_vector_index_tids(share::schema::ObSchemaGetterGuard *schema_guard,
                                            const ObTableSchema &data_table_schema,
                                            const ObIndexType index_type,
                                            const int64_t col_id,
                                            ObIArray<IvfIndexTableInfo> &tids)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  if (OB_ISNULL(schema_guard) || !share::schema::is_local_vec_ivf_index(index_type) || !data_table_schema.is_user_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard), K(index_type), K(data_table_schema));
  } else if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
    const ObTableSchema *index_table_schema = nullptr;
    if (OB_FAIL(schema_guard->get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_table_schema))) {
      LOG_WARN("fail to get index_table_schema", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
    } else if (index_table_schema->get_index_type() != index_type) {
    } else {
      bool has_same_col_id = false;
      for (int64_t j = 0; OB_SUCC(ret) && j < index_table_schema->get_column_count() && !has_same_col_id; j++) {
        const ObColumnSchemaV2 *col_schema = nullptr;
        if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
        } else if (!col_schema->is_vec_ivf_center_id_column() &&
                  !index_table_schema->is_vec_ivfsq8_meta_index() &&
                  !index_table_schema->is_vec_ivfpq_pq_centroid_index() &&
                  !index_table_schema->is_vec_ivfpq_code_index() &&
                  !index_table_schema->is_vec_ivfpq_rowkey_cid_index()) {
        } else if (OB_FAIL(has_same_cascaded_col_id(data_table_schema, *col_schema, col_id, has_same_col_id))) {
          LOG_WARN("fail to do has_same_cascaded_col_id", K(ret), K(col_id));
        } else if (has_same_col_id) {
          if (OB_FAIL(tids.push_back(IvfIndexTableInfo(simple_index_infos.at(i).table_id_, index_table_schema->get_schema_version())))) {
            LOG_WARN("fail to push back <tid, index status> pair", K(ret),
                K(simple_index_infos.at(i).table_id_), K(i), K(index_table_schema->get_schema_version()));
          }
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::update_param_extra_actual_size(const ObTableSchema &data_schema, ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  const ObString &old_param_str = index_schema.get_index_params();
  if (!old_param_str.empty() && share::schema::is_vec_hnsw_index(index_schema.get_index_type())) {
    ObVectorIndexParam old_param;
    if (OB_FAIL(parser_params_from_string(old_param_str, ObVectorIndexType::VIT_HNSW_INDEX, old_param))) {
      LOG_WARN("failed to parser_params_from_string", K(ret), K(old_param_str));
    } else {
      int64_t old_extra_info_actual_size = old_param.extra_info_actual_size_;
      int64_t new_extra_info_actual_size = 0;
      int64_t extra_info_max_size = old_param.extra_info_max_size_;
      if (extra_info_max_size <= 0) {
        // do nothing, extra_info is closed
      } else if (data_schema.get_index_type() != ObIndexType::INDEX_TYPE_IS_NOT) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("must not index table", K(ret), K(data_schema));
      } else if (OB_FAIL(check_extra_info_size(data_schema, nullptr /*seesion_info*/, true /*is_set_extra_info*/,
                                               extra_info_max_size, new_extra_info_actual_size))) {
        LOG_WARN("fail to check extra info size", K(ret), K(extra_info_max_size));
      } else if (old_extra_info_actual_size == new_extra_info_actual_size) {
         // do nothing, extra_info_actual_size is not change
      } else {
        int64_t pos = 0;
        const char *to_filter= "EXTRA_INFO_ACTUAL_SIZE";
        char new_params_str[OB_MAX_TABLE_NAME_LENGTH];
        int32_t res_len = 0;
        if (OB_FAIL(filter_index_param(old_param_str, to_filter, new_params_str, res_len))) {
          LOG_WARN("fail to filter index param", K(ret), K(old_param_str));
        } else if (OB_FALSE_IT(pos = res_len)) {
        } else if (new_extra_info_actual_size > 0 &&
                   OB_FAIL(databuff_printf(new_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
                                           ", EXTRA_INFO_ACTUAL_SIZE=%ld", new_extra_info_actual_size))) {
          LOG_WARN("fail to printf databuff", K(ret));
        } else {
          ObString new_index_params;
          new_index_params.assign_ptr(new_params_str, pos);
          if (OB_FAIL(index_schema.set_index_params(new_index_params))) {
            LOG_WARN("fail to set index params", K(ret), K(new_index_params));
          } else {
            LOG_DEBUG("vector index params", K(new_index_params));
          }
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::check_extra_info_size(const ObTableSchema &tbl_schema,
                                             const sql::ObSQLSessionInfo *session_info, bool is_extra_max_size_set,
                                             int64_t extra_info_max_size, int64_t &extra_info_actual_size)
{
  int ret = OB_SUCCESS;
  if (!is_extra_max_size_set &&OB_ISNULL(session_info)){
    // !is_extra_max_size_set and session_info is null, means not use extra_info
    extra_info_actual_size = 0;
  } else if (is_extra_max_size_set && extra_info_max_size == 0) {
    extra_info_actual_size = 0;
  } else {
    const common::ObRowkeyInfo &rowkey_info = tbl_schema.get_rowkey_info();
    int64_t rowkey_size = 0;
    ObSEArray<ObVecExtraInfoObj, 4> extra_objs;
    ObVecExtraInfoObj tmp_obj;
    ObRowkeyColumn rowkey_column;
    bool is_column_valid = true;
    for (int64_t i = 0; i < rowkey_info.get_size() && OB_SUCC(ret) && is_column_valid; i++) {
      if (OB_FAIL(rowkey_info.get_column(i, rowkey_column))) {
        LOG_WARN("fail to get rowkey column", K(ret), K(i));
      } else if (!ObVecExtraInfo::is_obj_type_supported(rowkey_column.type_.get_type())) {
        is_column_valid = false;
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unsupported column type", K(ret), K(rowkey_column.type_));
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "extra_info not support column type");
      } else {
        common::ObObjDatumMapType obj_map_type =
            common::ObDatum::get_obj_datum_map_type(rowkey_column.type_.get_type());
        tmp_obj.obj_map_type_ = obj_map_type;
        if (ObVecExtraInfo::is_fixed_length_type(obj_map_type)) {
          rowkey_size += ObVecExtraInfo::FIXED_TYPE_LENGTH;
          tmp_obj.len_ = ObVecExtraInfo::FIXED_TYPE_LENGTH;
        } else {
          rowkey_size += rowkey_column.length_;
          tmp_obj.len_ = rowkey_column.length_;
        }
        // for get extra_info actual_size
        if (OB_FAIL(extra_objs.push_back(tmp_obj))) {
          LOG_WARN("push back failed", K(ret), K(tmp_obj));
        }
      }
    }
    if (OB_FAIL(ret)) {
      if (!is_column_valid && !is_extra_max_size_set) {
        extra_info_actual_size = 0;
        ret = OB_SUCCESS;
        LOG_INFO("not set extra_max_size, and rowkey_type not support");
      } else {
        LOG_WARN("fail to check extra info size", K(ret), K(rowkey_size), K(extra_info_max_size));
      }
    } else {
      extra_info_actual_size = ObVecExtraInfo::get_encode_size(extra_objs);
      LOG_INFO("get extra info actual size", K(extra_info_actual_size), K(rowkey_size), K(extra_info_max_size));
      if (is_extra_max_size_set) {
        if (extra_info_max_size != 1 && rowkey_size > extra_info_max_size) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("rowkey_size > extra_info_max_size", K(ret), K(rowkey_size), K(extra_info_max_size));
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "rowkey_size > extra_info_max_size");
        }
      } else {
        // !is_extra_max_size_set, means use seesion extra param
        uint64_t session_extra_info_max_size = 0;
        // todo(wmj): not use session extra param, because some mr not ready to use extra_info
        // ***if use session extra param, need update extra_info_max_size in index param***
        // if (OB_FAIL(session_info.get_ob_hnsw_extra_info_max_size(session_extra_info_max_size))) {
        //   LOG_WARN("fail to get session extra info max size", K(ret));
        // }
        if (extra_info_actual_size > session_extra_info_max_size) {
          LOG_INFO("extra_info_actual_size larger than session_extra_info_max_size", K(extra_info_actual_size), K(rowkey_size), K(session_extra_info_max_size));
          extra_info_actual_size = 0;
        }
      }
    }
  }
  return ret;
}

// get hnsw index tables
typedef common::ObBinaryHeap<ObVecTidCandidate, ObVecTidCandidateMaxCompare, 4> ObVecTidMaxHeap;

int ObVectorIndexUtil::get_latest_avaliable_index_tids_for_hnsw(
    share::schema::ObSchemaGetterGuard *schema_guard,
    const ObTableSchema &data_table_schema,
    const int64_t col_id,
    uint64_t &inc_tid,
    uint64_t &vbitmap_tid,
    uint64_t &snapshot_tid)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();

  inc_tid = OB_INVALID_ID;
  vbitmap_tid = OB_INVALID_ID;
  snapshot_tid = OB_INVALID_ID;

  ObVecTidCandidateMaxCompare cmp;
  ObVecTidMaxHeap inc_tid_heap(cmp);

  if (OB_ISNULL(schema_guard) || !data_table_schema.is_user_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard), K(data_table_schema));
  } else if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  } else {

    // 1. get all delta buffer table tid by column id, need to modify if support multiple hnsw index on the same column.
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_table_schema = nullptr;
      if (OB_FAIL(schema_guard->get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_table_schema))) {
        LOG_WARN("fail to get index_table_schema", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
      } else if (OB_ISNULL(index_table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
      } else if (index_table_schema->get_index_type() == INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL) {
        bool has_same_col_id = false;
        for (int64_t j = 0; OB_SUCC(ret) && j < index_table_schema->get_column_count(); j++) {
          if (!index_table_schema->can_read_index() || !index_table_schema->is_index_visible()) {
            // skip unavaliable index table
          } else {
            const ObColumnSchemaV2 *col_schema = nullptr;
            if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected col schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
            } else if (!col_schema->is_vec_hnsw_vector_column()) {
              // only need vec_vector column, here skip other column
            } else if (OB_FAIL(has_same_cascaded_col_id(data_table_schema, *col_schema, col_id, has_same_col_id))) {
              LOG_WARN("fail to check same cascaded col id", K(ret), K(col_id));
            } else if (has_same_col_id) {
              ObString index_prefix;
              if (OB_FAIL(ObPluginVectorIndexUtils::get_vector_index_prefix(*index_table_schema, index_prefix))) {
                LOG_WARN("failed to get index prefix", K(ret));
              } else {
                ObVecTidCandidate candidate(index_table_schema->ObMergeSchema::get_schema_version(),
                                            simple_index_infos.at(i).table_id_,
                                            index_prefix);
                if (OB_FAIL(inc_tid_heap.push(candidate))) {
                  LOG_WARN("failed to push candidate to inc table id heap", K(ret));
                }
              }
            }
          }
        }
      }
    }

    // 2. get latest and avaliable table id groups
    for (int64_t idx = 0;
         idx < inc_tid_heap.count() && OB_SUCC(ret) &&  (vbitmap_tid == OB_INVALID_ID || snapshot_tid == OB_INVALID_ID);
         idx++ ) {
      // check index prefix by schema version order.
      ObString &current_index_prefix = inc_tid_heap.at(idx).index_prefix_;
      inc_tid = inc_tid_heap.at(idx).inc_tid_;
      vbitmap_tid = OB_INVALID_ID;
      snapshot_tid = OB_INVALID_ID;

      for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
        const ObTableSchema *index_table_schema = nullptr;
        if (OB_FAIL(schema_guard->get_table_schema(tenant_id, simple_index_infos.at(i).table_id_, index_table_schema))) {
          LOG_WARN("fail to get index_table_schema", K(ret), K(tenant_id), "table_id", simple_index_infos.at(i).table_id_);
        } else if (OB_ISNULL(index_table_schema)) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("index table schema should not be null", K(ret), K(simple_index_infos.at(i).table_id_));
        } else if (!(index_table_schema->get_index_type() == INDEX_TYPE_VEC_INDEX_ID_LOCAL
                   || index_table_schema->get_index_type() == INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL)) {
          // skip if index_table_schema is not 4, 5 index table
        } else if (!index_table_schema->can_read_index() || !index_table_schema->is_index_visible()) {
          // skip unavaliable index table
        } else {
          ObString index_prefix;
          if (OB_FAIL(ObPluginVectorIndexUtils::get_vector_index_prefix(*index_table_schema, index_prefix))) {
            LOG_WARN("failed to get index prefix", K(ret));
          } else if (index_prefix == current_index_prefix) {
            bool has_same_col_id = false;
            for (int64_t j = 0; OB_SUCC(ret) && j < index_table_schema->get_column_count(); j++) {
              const ObColumnSchemaV2 *col_schema = nullptr;
              if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected col schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
              } else if (!col_schema->is_vec_hnsw_vector_column()) {
                // only need vec_vector column, here skip other column
              } else if (OB_FAIL(has_same_cascaded_col_id(data_table_schema, *col_schema, col_id, has_same_col_id))) {
                LOG_WARN("fail to check same cascaded col id", K(ret), K(col_id));
              } else if (has_same_col_id) {
                if (index_table_schema->get_index_type() == INDEX_TYPE_VEC_INDEX_ID_LOCAL) {
                  vbitmap_tid = simple_index_infos.at(i).table_id_;
                } else if (index_table_schema->get_index_type() == INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL) {
                  snapshot_tid = simple_index_infos.at(i).table_id_;
                }
              }
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (inc_tid == OB_INVALID_ID || vbitmap_tid == OB_INVALID_ID || snapshot_tid == OB_INVALID_ID) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get latest avaliable index tids for hnsw", K(ret), K(inc_tid_heap.count()));
    } else {
      LOG_DEBUG("get latest avaliable index tids for hnsw", K(inc_tid), K(vbitmap_tid), K(snapshot_tid));
    }
  }

  return ret;
}

int ObVectorIndexUtil::get_vector_index_tid_with_index_prefix(
    share::schema::ObSchemaGetterGuard *schema_guard,
    const ObTableSchema &data_table_schema,
    const ObIndexType index_type,
    const int64_t col_id,
    ObString &index_prefix,
    uint64_t &tid)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
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
      } else if (index_table_schema->get_index_type() != index_type) {
        // skip not spec index type
      } else if (index_table_schema->is_vec_rowkey_vid_type() || index_table_schema->is_vec_vid_rowkey_type()) {
        // rowkey_vid and vid_rowkey is shared, only one, just return
        tid = simple_index_infos.at(i).table_id_;
      } else { // delta buffer, index id, index snapshot, check index prefix and cascaded_column by vec_vector col
        ObString current_index_prefix;
        if (OB_FAIL(ObPluginVectorIndexUtils::get_vector_index_prefix(*index_table_schema, current_index_prefix))) {
          LOG_WARN("failed to get index prefix", K(ret));
        } else if (index_prefix == current_index_prefix) {
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
  }
  return ret;
}

int ObVectorIndexUtil::check_vec_index_param(
    const uint64_t tenant_id, const ParseNode *option_node,
    common::ObIAllocator &allocator, const ObTableSchema &tbl_schema,
    ObString &index_params, ObString &vec_column_name, ObIndexType &vec_index_type, sql::ObSQLSessionInfo *session_info)
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
    bool is_sparse_vec_col = false;
    const ObColumnSchemaV2 *col_schema = nullptr;
    if(OB_ISNULL(col_schema = tbl_schema.get_column_schema(vec_column_name))){
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column schema", K(ret), KP(col_schema));
    } else if (!col_schema->is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argumnet", K(ret), KP(col_schema));
    } else if (OB_FAIL(ObVectorIndexUtil::is_sparse_vec_col(col_schema->get_extended_type_info(), is_sparse_vec_col))) {
      LOG_WARN("fail to check is sparse vec col", K(ret));
    }

    if (OB_FAIL(ret)){
    } else if (!is_sparse_vec_col && OB_FAIL(ObVectorIndexUtil::get_vector_dim_from_extend_type_info(col_schema->get_extended_type_info(), vector_dim))) {
      LOG_WARN("fail to get vector dim", K(ret), K(col_schema));
    } else {
      tmp_str.assign_ptr(index_param_str, index_param_length);
      if (OB_ISNULL(option_node->children_[0])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("children can't be null", K(ret));
      } else if (OB_FAIL(ObVectorIndexUtil::insert_index_param_str(tmp_str, allocator, index_params))) {
        LOG_WARN("write string failed", K(ret), K(tmp_str), K(index_params));
      } else if (OB_FAIL(check_index_param(option_node, allocator, vector_dim, is_sparse_vec_col, index_params, vec_index_type, tbl_schema, session_info))) {
        LOG_WARN("fail to check vector index definition", K(ret));
      } else if (share::schema::is_vec_ivf_index(vec_index_type)) {
        int64_t lob_inrow_threshold = tbl_schema.get_lob_inrow_threshold();
        int64_t max_vec_len = 4 * vector_dim;
        if (OB_SUCC(ret) && lob_inrow_threshold < max_vec_len) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("create ivf index on column with outrow lob data not supported", K(ret), K(vector_dim), K(lob_inrow_threshold));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "create ivf index on column with outrow lob data is");
        }
        if (OB_SUCC(ret)) {
          bool is_data_table_column_store = false;
          if (OB_FAIL(tbl_schema.get_is_column_store(is_data_table_column_store))) {
            LOG_WARN("fail to get is column store", K(tbl_schema));
          } else if (is_data_table_column_store) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("create ivf index on table with column store not supported", K(ret), K(tbl_schema));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "create ivf index on table with column store is");
          }
        }
      } else if (share::schema::is_vec_spiv_index(vec_index_type)) {
        // do nothing
      }
    }
  }
  return ret;
}

// for ivf
int ObVectorIndexUtil::get_vector_index_tid_check_valid(
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
      } else if (!index_table_schema->can_read_index()) {
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && tid == OB_INVALID_ID && j < index_table_schema->get_column_count(); j++) {
          const ObColumnSchemaV2 *col_schema = nullptr;
          if (OB_ISNULL(col_schema = index_table_schema->get_column_schema_by_idx(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected col_schema, is nullptr", K(ret), K(j), KPC(index_table_schema));
          } else if (!col_schema->is_vec_ivf_center_id_column() && !col_schema->is_vec_ivf_pq_center_ids_column()) {
          } else if (vec_cid_col_id == col_schema->get_column_id()) {
            tid = index_table_schema->get_table_id();
          }
        }
      }
    }
  }
  return ret;
}

int ObVectorIndexUtil::get_vector_index_param_with_dim(
      share::schema::ObSchemaGetterGuard &schema_guard,
      uint64_t tenant_id,
      int64_t index_table_id,
      int64_t data_table_id,
      ObVectorIndexType index_type,
      ObVectorIndexParam &param)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *index_table_schema = nullptr;
  const ObTableSchema *data_table_schema = nullptr;
  if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id), K(index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
    LOG_WARN("failed to get simple schema", KR(ret), K(tenant_id), K(data_table_id));
  } else if (OB_ISNULL(index_table_schema) || OB_ISNULL(data_table_schema)) {
    ret = OB_ERR_NULL_VALUE;
    LOG_WARN("invalid null table schema", K(ret), KP(index_table_schema), KP(data_table_schema));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
                 index_table_schema->get_index_params(), index_type, param))) {
    LOG_WARN("fail to parse params from string", K(ret), K(index_table_schema->get_index_params()));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(
        *index_table_schema, *data_table_schema, param.dim_))) {
    LOG_WARN("fail to get vec_index_col_param", K(ret));
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
    ObVectorIndexParam &param,
    bool &param_filled)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
  const int64_t tenant_id = data_table_schema.get_tenant_id();
  param_filled = false;
  if (OB_ISNULL(schema_guard) || !data_table_schema.is_user_table()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(schema_guard), K(data_table_schema));
  } else if (OB_FAIL(data_table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count() && !param_filled; ++i) {
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
        for (int64_t j = 0; OB_SUCC(ret) && j < index_table_schema->get_column_count() && !param_filled; j++) {
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
              for (int64_t k = 0; OB_SUCC(ret) && k < cascaded_column_ids.count() && !param_filled; ++k) {
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
                    param_filled = true;
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
    const ParseNode *option_node,
    common::ObIAllocator &allocator,
    const int64_t vector_dim,
    const bool is_sparse_vec,
     ObString &index_params,
     ObIndexType &out_index_type,
     const ObTableSchema &tbl_schema,
     sql::ObSQLSessionInfo *session_info)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  if (OB_ISNULL(option_node) || OB_ISNULL(session_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse node or session_info", K(ret), KP(option_node), KP(session_info));
  } else if (option_node->type_ != T_VEC_INDEX_PARAMS) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected parse node type", K(ret), K(option_node->type_));
  } else if (OB_ISNULL(option_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("option_node child is null", K(ret), KP(option_node->children_[0]));
  } else if (!is_sparse_vec && vector_dim <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector dim", K(ret), K(vector_dim));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else {
    if (!is_sparse_vec && (option_node->num_child_ < 4 || option_node->num_child_ % 2 !=  0)) {  // at least distance and type should be set
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid vector param num", K(ret), K(option_node->num_child_));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "vector index params not set distance and type is");
    } else if (is_sparse_vec) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not sopport sparse vector index", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "sparse vector index is");
    }
    ObString last_variable;
    ObString key_parser_name;
    ObString data_parser_name;
    ObString new_variable_name;
    ObString new_parser_name;
    ObString lib_name;
    ObString distance_name;
    ObString type_name;
    int64_t parser_value = 0;
    int32_t str_len = 0;
    int64_t m_value = 0;
    int64_t ef_construction_value = 0;
    int64_t sample_per_nlist_value = 0;
    int64_t extra_info_max_size = 0;
    int64_t nlist_value = 0;
    int64_t nbits_value = 0;

    bool distance_is_set = false;       // ivf/hnsw/spiv
    bool lib_is_set = false;            // ivf/hnsw
    bool type_hnsw_is_set = false;      // ivf/hnsw
    bool m_is_set = false;              // ivf/hnsw
    bool ef_construction_is_set = false;// hnsw
    bool ef_search_is_set = false;      // hnsw
    bool extra_info_max_size_is_set = false; // hnsw
    bool nlist_is_set = false;          // ivf
    bool sample_per_nlist_is_set = false; // ivf
    bool nbits_is_set = false;          // ivf
    bool type_ivf_flat_is_set = false;  // ivf
    bool type_ivf_sq8_is_set = false;   // ivf
    bool type_ivf_pq_is_set = false;    // ivf
    bool type_hnsw_bq_is_set = false;   // hnsw_bq
    bool refine_type_is_set = false; // hnsw_bq
    bool refine_k_is_set = false;    // hnsw_bq
    bool bq_use_fht_is_set = false;    // hnsw_bq
    bool bq_bits_query_set = false;    // hnsw_bq

    // [4.3.5.3, 4.4.0.0) or [4.4.1.0, )
    bool is_enable_bp_param = (tenant_data_version >= MOCK_DATA_VERSION_4_3_5_3 && tenant_data_version < DATA_VERSION_4_4_0_0)
        || (tenant_data_version >= DATA_VERSION_4_4_1_0);
    bool is_enable_bp_cosine_and_ip = is_enable_bp_param;

    const int64_t default_m_value = 16;
    const int64_t default_ef_construction_value = 200;
    const int64_t default_ef_search_value = 64;
    const int64_t default_nlist_value = 128;
    const int64_t default_sample_per_nlist_value = 256;
    const int64_t default_nbits_value = 8;
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
                   new_variable_name != "SAMPLE_PER_NLIST" &&
                   new_variable_name != "EXTRA_INFO_MAX_SIZE" &&
                   new_variable_name != "REFINE_TYPE" &&
                   new_variable_name != "BQ_BITS_QUERY" &&
                   new_variable_name != "REFINE_K" &&
                   new_variable_name != "BQ_USE_FHT" &&
                   new_variable_name != "NBITS") {
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
          // for float data
          str_len = static_cast<int32_t>(option_node->children_[i]->str_len_);
          data_parser_name.assign_ptr(option_node->children_[i]->str_value_, str_len);
          new_parser_name = data_parser_name;
        } else if (option_node->children_[i]->type_ == T_BOOL) {
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
            if (is_sparse_vec && new_parser_name != "INNER_PRODUCT") {
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("not support sparse vector index distance algorithm", K(ret), K(new_parser_name));
              LOG_USER_ERROR(OB_NOT_SUPPORTED, "this type of sparse vector index distance algorithm is");
            } else {
              distance_is_set = true;
              distance_name = new_parser_name;
            }
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
          } else if (new_parser_name == "HNSW_BQ") {
            type_hnsw_is_set = true;
            type_hnsw_bq_is_set = true;
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
          if (parser_value >= 1 && parser_value <= 65536 ) {
            nlist_is_set = true;
            nlist_value = parser_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid vector index nlist value", K(ret), K(parser_value));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index nlist is");
          }
        } else if (last_variable == "SAMPLE_PER_NLIST") {
          if (parser_value >= 1 && parser_value <= INT64_MAX ) {
            sample_per_nlist_is_set = true;
            sample_per_nlist_value = parser_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid vector index sample_per_nlist value", K(ret), K(parser_value));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index sample_per_nlist is");
          }
        } else if (last_variable == "EXTRA_INFO_MAX_SIZE") {
          if (parser_value >= 0 && parser_value <= ObVecExtraInfo::EXTRA_INFO_PARAM_MAX_VALUE) {
            extra_info_max_size_is_set = true;
            extra_info_max_size = parser_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid vector index extra_info_max_size value", K(ret), K(parser_value));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index extra_info_max_size is");
          }
        } else if (last_variable == "REFINE_TYPE") {
          if (! is_enable_bp_param || ! (new_parser_name == "FP32" || new_parser_name == "SQ8")) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index vector index refine_type value", K(ret), K(is_enable_bp_param), K(new_parser_name));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index refine_type is");
          } else {
            refine_type_is_set = true;
          }
        } else if (last_variable == "BQ_BITS_QUERY") {
          if (! is_enable_bp_param || (parser_value != 4 && parser_value != 32)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index bq_bits_query value", K(ret), K(is_enable_bp_param), K(parser_value));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index bq_bits_query is");
          } else {
            bq_bits_query_set = true;
          }
        } else if (last_variable == "REFINE_K") {
          int err = 0;
          char *endptr = NULL;
          double out_val = ObCharset::strntod(new_parser_name.ptr(), new_parser_name.length(), &endptr, &err);
          if (err != 0 || (new_parser_name.ptr() + new_parser_name.length()) != endptr) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("fail to cast string to double", K(ret), K(new_parser_name), K(err), KP(endptr));
          } else if (! is_enable_bp_param || (out_val < 1.0 || out_val > 1000)) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index refine_k value", K(ret), K(is_enable_bp_param), K(out_val), K(new_parser_name));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index refine_k is");
          } else {
            refine_k_is_set = true;
          }
        } else if (last_variable == "BQ_USE_FHT") {
          if (! is_enable_bp_param || (! (0 == parser_value || 1 == parser_value))) {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("not support vector index bq_use_fht value", K(ret), K(is_enable_bp_param), K(parser_value));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index bq_use_fht is");
          } else {
            bq_use_fht_is_set = true;
          }
        } else if (last_variable == "NBITS") {
          if (parser_value >= 1 && parser_value <= 24) {
            nbits_is_set = true;
            nbits_value = parser_value;
          } else {
            ret = OB_NOT_SUPPORTED;
            LOG_WARN("invalid vector index nbits value", K(ret), K(parser_value));
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index nbits is");
          }
        } else {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not support vector index param", K(ret), K(last_variable));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "this value of vector index ef_search is");
        }
      }
    }

    if (OB_SUCC(ret) && type_hnsw_bq_is_set) {
      if (tenant_data_version < DATA_VERSION_4_3_5_2) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("for hnsw bq index current version is not support", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "for hnsw bq index current version is");
      } else if (type_hnsw_bq_is_set && ! is_enable_bp_cosine_and_ip && distance_name != "L2") {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not support distance algorithm for hnsw bq index", K(ret), K(distance_name));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "current distance algorithm for hnsw bq index is");
      }
    }

    if (OB_SUCC(ret) && ! type_hnsw_bq_is_set
        && (refine_type_is_set || refine_k_is_set
            || bq_use_fht_is_set || bq_bits_query_set)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support parameter for current index", K(ret),
          K(refine_type_is_set), K(refine_k_is_set), K(bq_use_fht_is_set), K(bq_bits_query_set));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "parameter for current index is");
    }
    if (OB_SUCC(ret) && extra_info_max_size_is_set) {
      ObDocIDType vid_type = ObDocIDType::INVALID;
      if (tenant_data_version < DATA_VERSION_4_3_5_2) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "vec hnsw extra_info before 4.3.5.2 is");
        LOG_WARN("vec hnsw index extra_info is not supported before 4.3.5.2", K(ret), K(tenant_data_version));
      } else if (OB_FAIL(ObVectorIndexUtil::determine_vid_type(tbl_schema, vid_type))) {
        LOG_WARN("Failed to check vid type", K(ret));
      } else if (vid_type == ObDocIDType::HIDDEN_INC_PK) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "vec hnsw extra_info on table with pk increment is");
        LOG_WARN("vec hnsw index extra_info is not supported on table with pk increment", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!is_sparse_vec) {
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
        } else if (!type_ivf_pq_is_set && nbits_is_set) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ivf vector index param nbits only need to be set of ivf_pq mode", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ivf vector index param nbits to be set in ivf_sq8 or ivf_flat is");
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
        } else if (ef_construction_is_set || ef_search_is_set || extra_info_max_size_is_set) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ivf vector index param ef_construction or ef_search or extra_info_max_size should not be set", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ivf vector index param setting ef_construction or ef_search should or extra_info_max_size is");
        }
        nlist_value = nlist_is_set ? nlist_value : default_nlist_value;
        sample_per_nlist_value = sample_per_nlist_is_set ? sample_per_nlist_value : default_sample_per_nlist_value;
        nbits_value = nbits_is_set ? nbits_value : default_nbits_value;
        if (OB_FAIL(ret)) {
        } else if (INT64_MAX / sample_per_nlist_value < nlist_value) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("ivf vector index param nlist_value * sample_per_nlist_value should less than int64_max", K(ret), K(nlist_value), K(sample_per_nlist_value));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ivf vector index param nlist_value * sample_per_nlist_value should less than int64_max");
        } else if (INT64_MAX / sample_per_nlist_value < (1L << nbits_value)) {
           ret = OB_NOT_SUPPORTED;
          LOG_WARN("ivf vector index param (1L << nbits_value) * sample_per_nlist_value should less than int64_max", K(ret), K(nbits_value), K(sample_per_nlist_value));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "ivf vector index param (1L << nbits_value) * sample_per_nlist_value should less than int64_max");
        }
      } else if (hnsw_is_set) {
        ef_construction_value = ef_construction_is_set ? ef_construction_value : default_ef_construction_value;
        m_value = m_is_set ? m_value : default_m_value;
        if (m_value >= 5 && m_value <= 128) {
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
        } else if (nlist_is_set || sample_per_nlist_is_set || nbits_is_set) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("hnsw vector index no need to set nlist or sample_per_nlist or nbits",
            K(ret), K(nlist_is_set), K(sample_per_nlist_is_set));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "hnsw vector index setting nlist or sample_per_nlist or nbits is");
        }
      }
      if (OB_SUCC(ret)) {
        char not_set_params_str[OB_MAX_TABLE_NAME_LENGTH];
        const ObString default_lib = ivf_is_set ? "OB" : "VSAG";
        int64_t pos = 0;
        int64_t extra_info_actual_size = 0;
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
        } else if (!nbits_is_set && type_ivf_pq_is_set &&
                   OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
                                           ", NBITS=%ld", default_nbits_value))) {
          LOG_WARN("fail to printf databuff", K(ret));
        } else if (hnsw_is_set && OB_FAIL(check_extra_info_size(tbl_schema, session_info, extra_info_max_size_is_set,
                                                                extra_info_max_size, extra_info_actual_size))) {
          LOG_WARN("check_extra_info_size failed", K(ret), K(extra_info_max_size), K(tbl_schema));
        } else if (hnsw_is_set && extra_info_actual_size > 0 && OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos,
                                                          ", EXTRA_INFO_ACTUAL_SIZE=%ld", extra_info_actual_size))) {
          LOG_WARN("fail to printf databuff", K(ret));
        } else if (extra_info_actual_size > 0 && extra_info_max_size <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("extra_info_actual_size > 0 && extra_info_max_size <= 0", K(ret), K(extra_info_actual_size), K(extra_info_max_size));
        } else if (is_enable_bp_param && type_hnsw_bq_is_set &&! refine_type_is_set &&
            OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos, ", REFINE_TYPE=SQ8"))) {
          LOG_WARN("fail to printf databuff", K(ret));
        } else if (is_enable_bp_param && type_hnsw_bq_is_set && ! bq_use_fht_is_set
            && OB_FAIL(databuff_printf(not_set_params_str, OB_MAX_TABLE_NAME_LENGTH, pos, ", BQ_USE_FHT=TRUE"))) {
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
    } else if (is_sparse_vec) {
      if (!distance_is_set) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("sparse vector index distance algorithm need to be set 'INNER_PRODUCT'", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "sparse vector index distance algorithm need to be set 'INNER_PRODUCT'");
      } else {
        if (lib_is_set
            || type_hnsw_is_set
            || m_is_set
            || ef_construction_is_set
            || ef_search_is_set
            || nlist_is_set
            || nbits_is_set
            || sample_per_nlist_is_set
            || type_ivf_flat_is_set
            || type_ivf_sq8_is_set
            || type_ivf_pq_is_set
            || extra_info_max_size_is_set) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("sparce vector index only support to set distance algorithm", K(ret));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "sparce vector index only support to set distance algorithm");
        }
      }
    }

    out_index_type = INDEX_TYPE_MAX;
    if (OB_FAIL(ret)) {
    } else if (is_sparse_vec) {
      out_index_type = INDEX_TYPE_VEC_SPIV_DIM_DOCID_VALUE_LOCAL;
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

void ObVectorIndexUtil::save_column_schema(
    const ObColumnSchemaV2 *&old_column,
    const ObColumnSchemaV2 *&new_column,
    const ObColumnSchemaV2 *cur_column)
{
  if (OB_ISNULL(cur_column)) {
  } else if (OB_ISNULL(old_column)) {
    old_column = cur_column;
  } else if (old_column->get_column_id() > cur_column->get_column_id()) {
    new_column = old_column;
    old_column = cur_column;
  } else {
    new_column = cur_column;
  }
}

int ObVectorIndexUtil::construct_new_column_schema_from_exist(
    const ObColumnSchemaV2 *old_column_ptr,
    const ObColumnSchemaV2 *&new_column_ptr,
    const VecColType col_type,
    ObColumnSchemaV2 &new_column,
    uint64_t &available_col_id)
{
  int ret = OB_SUCCESS;
  char col_name_buf[OB_MAX_COLUMN_NAME_LENGTH] = {'\0'};
  int64_t name_pos = 0;
  ObSEArray<uint64_t, 4> cascaded_col_ids;
  if (OB_ISNULL(old_column_ptr)) {
  } else if (OB_FAIL(new_column.assign(*old_column_ptr))) {
    LOG_WARN("failed to assign column schema", K(ret));
  } else if (OB_FAIL(old_column_ptr->get_cascaded_column_ids(cascaded_col_ids))) {
    LOG_WARN("fail to get cascaded column ids", K(ret), KPC(old_column_ptr));
  } else if (OB_FAIL(ObVecIndexBuilderUtil::construct_ivf_col_name(cascaded_col_ids, col_type, col_name_buf, OB_MAX_COLUMN_NAME_LENGTH, name_pos))) {
    LOG_WARN("failed to construct ivf column name", K(ret), K(col_name_buf));
  } else {
    new_column.set_column_name(col_name_buf);
    new_column.set_prev_column_id(UINT64_MAX);
    new_column.set_next_column_id(UINT64_MAX);
    new_column.set_column_id(available_col_id++);
    new_column_ptr = &new_column;
  }
  return ret;
}

int ObVectorIndexUtil::set_new_index_column(
    ObTableSchema &new_index_schema,
    const ObColumnSchemaV2 *old_column_ptr,
    const ObColumnSchemaV2 *&new_column_ptr)
{
  int ret = OB_SUCCESS;
  ObColumnSchemaV2 *index_column = nullptr;
  if (OB_ISNULL(old_column_ptr) || OB_ISNULL(new_column_ptr)) {
  } else if (OB_ISNULL(index_column = new_index_schema.get_column_schema(old_column_ptr->get_column_id()))) {
    // there may be no specified column in this index table.
  } else if (OB_FAIL(index_column->set_column_name(new_column_ptr->get_column_name_str()))) {
    LOG_WARN("fail to set column name", K(ret), KPC(index_column));
  } else {
    ObColumnSchemaV2 *prev_col = new_index_schema.get_column_schema(index_column->get_prev_column_id());
    ObColumnSchemaV2 *next_col = new_index_schema.get_column_schema(index_column->get_next_column_id());
    index_column->set_column_id(new_column_ptr->get_column_id());
    if (OB_NOT_NULL(prev_col)) {
      prev_col->set_next_column_id(index_column->get_column_id());
    }
    if (OB_NOT_NULL(next_col)) {
      next_col->set_prev_column_id(index_column->get_column_id());
    }
    new_index_schema.set_max_used_column_id(max(new_index_schema.get_max_used_column_id(), index_column->get_column_id()));
  }
  return ret;
}

int ObVectorIndexUtil::reconstruct_ivf_index_schema_in_rebuild(
  rootserver::ObDDLSQLTransaction &trans,
  rootserver::ObDDLService &ddl_service,
  const obrpc::ObCreateIndexArg &create_index_arg,
  const ObTableSchema &data_table_schema,
  ObTableSchema &new_index_schema)
{
  int ret = OB_SUCCESS;
  const bool is_ivf_pq = new_index_schema.is_vec_ivfpq_index();
  const bool is_ivf_sq = new_index_schema.is_vec_ivfsq8_index();
  const ObColumnSchemaV2 *old_cid_column = nullptr;
  const ObColumnSchemaV2 *new_cid_column = nullptr;
  const ObColumnSchemaV2 *old_pq_cids_column = nullptr;
  const ObColumnSchemaV2 *new_pq_cids_column = nullptr;
  const ObColumnSchemaV2 *old_data_vec_column = nullptr;
  const ObColumnSchemaV2 *new_data_vec_column = nullptr;
  const ObColumnSchemaV2 *old_center_vec_column = nullptr;
  const ObColumnSchemaV2 *new_center_vec_column = nullptr;
  ObColumnSchemaV2 new_column_for_cid;
  ObColumnSchemaV2 new_column_for_pq_cids;
  ObColumnSchemaV2 new_column_for_data_vec;
  ObColumnSchemaV2 new_column_for_center_vec;
  schema::ColumnReferenceSet index_col_set;
  uint64_t available_col_id = data_table_schema.get_max_used_column_id() + 1;
  rootserver::ObDDLOperator ddl_operator(*GCTX.schema_service_, ddl_service.get_sql_proxy());
  if (OB_FAIL(ObVecIndexBuilderUtil::get_index_column_ids(data_table_schema, create_index_arg, index_col_set))) {
    LOG_WARN("fail to get index column ids", K(ret), K(data_table_schema), K(create_index_arg));
  }
  const ObColumnSchemaV2 *col_schema = nullptr;
  bool reseted = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < create_index_arg.index_columns_.count(); ++i) {
    const ObString &column_name = create_index_arg.index_columns_.at(i).column_name_;
    ObSEArray<uint64_t, 4> cascaded_col_ids;
    if (column_name.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column name is empty", K(ret), K(column_name));
    } else if (OB_ISNULL(col_schema = data_table_schema.get_column_schema(column_name))) {
      ret = OB_ERR_KEY_COLUMN_DOES_NOT_EXITS;
      LOG_USER_ERROR(OB_ERR_KEY_COLUMN_DOES_NOT_EXITS, column_name.length(), column_name.ptr());
    } else if (col_schema->is_vec_index_column()) {
      if (!reseted) {
        index_col_set.reset();
        reseted = true;
      }
      if (OB_FAIL(col_schema->get_cascaded_column_ids(cascaded_col_ids))) {
        LOG_WARN("fail to get cascaded column ids", K(ret), K(col_schema));
      } else {
        for (int i = 0; OB_SUCC(ret) && i < cascaded_col_ids.count(); i++) {
          if (OB_FAIL(index_col_set.add_member(cascaded_col_ids.at(i)))) {
            LOG_WARN("fail to add index column id", K(ret), K(cascaded_col_ids.at(i)));
          }
        } // end for.
      }
    }
  }
  for (ObTableSchema::const_column_iterator iter = data_table_schema.column_begin();
     OB_SUCC(ret) && iter != data_table_schema.column_end(); iter++) {
    const ObColumnSchemaV2 *column_schema = *iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, column schema is nullptr", K(ret), K(data_table_schema));
    } else if (column_schema->is_vec_ivf_center_id_column()
        || column_schema->is_vec_ivf_pq_center_ids_column()
        || column_schema->is_vec_ivf_data_vector_column()
        || column_schema->is_vec_ivf_center_vector_column()) {
      bool is_match = false;
      if (OB_FAIL(ObVecIndexBuilderUtil::check_index_match(*column_schema, index_col_set, is_match))) {
        LOG_WARN("fail to check index match", K(ret), KPC(column_schema), K(index_col_set));
      } else if (is_match) {
        if (column_schema->is_vec_ivf_center_id_column()) {
          save_column_schema(old_cid_column, new_cid_column, column_schema);
        } else if (column_schema->is_vec_ivf_pq_center_ids_column()) {
          save_column_schema(old_pq_cids_column, new_pq_cids_column, column_schema);
        } else if (column_schema->is_vec_ivf_data_vector_column() && new_index_schema.is_vec_ivfsq8_index()) {
          save_column_schema(old_data_vec_column, new_data_vec_column, column_schema);
        } else if (column_schema->is_vec_ivf_center_vector_column() && column_schema->get_column_name_str().prefix_match(OB_VEC_IVF_PQ_CENTER_VECTOR_COLUMN_NAME_PREFIX)) {
          save_column_schema(old_center_vec_column, new_center_vec_column, column_schema);
        }
      }
    }
  } // end for
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(old_cid_column)) {
  } else if (OB_ISNULL(new_cid_column)) {
    if (OB_FAIL(construct_new_column_schema_from_exist(old_cid_column, new_cid_column, IVF_CENTER_ID_COL, new_column_for_cid, available_col_id))) {
      LOG_WARN("failed to construct new column schema from exist", K(ret), K(new_column_for_cid), KPC(old_cid_column));
    } else if (OB_FAIL(ddl_operator.insert_single_column(trans, data_table_schema, new_column_for_cid))) {
      LOG_WARN("failed to insert single column", K(new_column_for_cid));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(old_pq_cids_column)) {
  } else if (OB_ISNULL(new_pq_cids_column)) {
    if (OB_FAIL(construct_new_column_schema_from_exist(old_pq_cids_column, new_pq_cids_column, IVF_PQ_CENTER_IDS_COL, new_column_for_pq_cids, available_col_id))) {
      LOG_WARN("failed to construct new column schema from exist", K(ret), K(new_column_for_pq_cids), KPC(old_pq_cids_column));
    } else if (OB_FAIL(ddl_operator.insert_single_column(trans, data_table_schema, new_column_for_pq_cids))) {
      LOG_WARN("failed to insert single column", K(new_column_for_pq_cids));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(old_data_vec_column)) {
  } else if (OB_ISNULL(new_data_vec_column)) {
    if (OB_FAIL(construct_new_column_schema_from_exist(old_data_vec_column, new_data_vec_column, IVF_SQ8_DATA_VECTOR_COL, new_column_for_data_vec, available_col_id))) {
      LOG_WARN("failed to construct new column schema from exist", K(ret), K(new_column_for_data_vec), KPC(old_data_vec_column));
    } else if (OB_FAIL(ddl_operator.insert_single_column(trans, data_table_schema, new_column_for_data_vec))) {
      LOG_WARN("failed to insert single column", K(new_column_for_data_vec));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(old_center_vec_column)) {
  } else if (OB_ISNULL(new_center_vec_column)) {
    if (OB_FAIL(construct_new_column_schema_from_exist(old_center_vec_column, new_center_vec_column, IVF_PQ_CENTER_VECTOR_COL, new_column_for_center_vec, available_col_id))) {
      LOG_WARN("failed to construct new column schema from exist", K(ret), K(new_column_for_center_vec), KPC(old_center_vec_column));
    } else if (OB_FAIL(ddl_operator.insert_single_column(trans, data_table_schema, new_column_for_center_vec))) {
      LOG_WARN("failed to insert single column", K(new_column_for_center_vec));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(old_cid_column) || OB_ISNULL(new_cid_column)) {
  } else if (OB_FAIL(set_new_index_column(new_index_schema, old_cid_column, new_cid_column))) {
    LOG_WARN("failed to set new index column schema", K(ret), KPC(old_cid_column), KPC(new_cid_column));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(old_pq_cids_column) || OB_ISNULL(new_pq_cids_column)) {
  } else if (OB_FAIL(set_new_index_column(new_index_schema, old_pq_cids_column, new_pq_cids_column))) {
    LOG_WARN("failed to set new index column schema", K(ret), KPC(old_pq_cids_column), KPC(new_pq_cids_column));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(old_data_vec_column) || OB_ISNULL(new_data_vec_column)) {
  } else if (OB_FAIL(set_new_index_column(new_index_schema, old_data_vec_column, new_data_vec_column))) {
    LOG_WARN("failed to set new index column schema", K(ret), KPC(old_data_vec_column), KPC(new_data_vec_column));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(old_center_vec_column) || OB_ISNULL(new_center_vec_column)) {
  } else if (OB_FAIL(set_new_index_column(new_index_schema, old_center_vec_column, new_center_vec_column))) {
    LOG_WARN("failed to set new index column schema", K(ret), KPC(old_center_vec_column), KPC(new_center_vec_column));
  }
  new_index_schema.sort_column_array_by_column_id();
  return ret;
}

int ObVectorIndexUtil::generate_index_schema_from_exist_table(
    rootserver::ObDDLSQLTransaction &trans,
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
  const ObString new_index_params = create_index_arg.vidx_refresh_info_.index_params_;
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
      if (!new_index_params.empty()) {
        new_index_schema.set_index_params(new_index_params);
        // only vec_delta_buffer_type\vec_index_id_type may need update extra_info columns.
        if (new_index_schema.is_vec_delta_buffer_type() || new_index_schema.is_vec_index_id_type()) {
          ObVectorIndexParam new_vec_param;
          if (OB_FAIL(parser_params_from_string(new_index_params, ObVectorIndexType::VIT_HNSW_INDEX,
                                                       new_vec_param))) {
            LOG_WARN("fail to parse new index parsm", K(ret), K(new_index_params));
          } else if (new_vec_param.extra_info_actual_size_ > 0) {
            // open extra_info need add data_rowkey columns in index schema.
            HEAP_VAR(ObRowDesc, row_desc)
            {
              // Note. need_set_rk must not false! otherwise, the row_desc needs to pre-add the primary key column of
              // index_schema.
              if (OB_FAIL(ObVecIndexBuilderUtil::set_extra_info_columns(
                      data_table_schema, row_desc, false /*need_set_rk*/, new_vec_param, new_index_schema))) {
                LOG_WARN("fail to set extra info columns", K(ret), K(new_vec_param));
              }
              if (FAILEDx(new_index_schema.sort_column_array_by_column_id())) {
                LOG_WARN("failed to sort column", K(ret));
              } else {
                LOG_INFO("succeed to set extra info table columns", K(new_index_schema));
              }
            }
          } else if (new_vec_param.extra_info_actual_size_ == 0) {
            // close extra_indo need del data_rowkey columns in index schema.
            if (OB_FAIL(
                    ObVecIndexBuilderUtil::del_extra_info_columns(data_table_schema, new_vec_param, new_index_schema))) {
              LOG_WARN("fail to del extra info columns", K(ret), K(new_vec_param));
            }
            if (FAILEDx(new_index_schema.sort_column_array_by_column_id())) {
              LOG_WARN("failed to sort column", K(ret));
            } else {
              LOG_INFO("succeed to del extra info columns", K(new_index_schema));
            }
          }
        }
      }
      new_index_schema.set_max_used_column_id(max(
      new_index_schema.get_max_used_column_id(), data_table_schema.get_max_used_column_id()));
      new_index_schema.set_table_id(new_index_table_id);
      new_index_schema.set_index_status(INDEX_STATUS_UNAVAILABLE);
      new_index_schema.set_table_state_flag(data_table_schema.get_table_state_flag());
      new_index_schema.set_exec_env(create_index_arg.vidx_refresh_info_.exec_env_);

      if (new_index_schema.is_vec_ivf_index()
          && OB_FAIL(reconstruct_ivf_index_schema_in_rebuild(trans, ddl_service, create_index_arg, data_table_schema, new_index_schema))) {
        LOG_WARN("failed to reconstruct ivf index schema in rebuild", K(ret), K(new_index_schema));
      } // end is_vec_ivf_index.
    }
  }
  LOG_DEBUG("generate_index_schema_from_exist_table", K(ret), K(new_index_params), K(new_index_table_name));
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

  bool is_spiv = false;
  bool spiv_dim_docid_value_is_valid = false;
  bool spiv_rowkey_docid_is_valid = false;
  bool spiv_docid_rowkey_is_valid = false;

  is_valid = false;

  if (index_column_name.empty() || OB_INVALID_ID == data_table_id ||
      OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == database_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(index_column_name), K(data_table_id), K(tenant_id), K(database_id));
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
        } else if (index_schema->is_vec_hnsw_index()) {
          if (index_schema->is_vec_rowkey_vid_type()) {
            rowkey_vid_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
          } else if (index_schema->is_vec_vid_rowkey_type()) {
            vid_rowkey_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
          } else if (index_schema->is_vec_delta_buffer_type()) {
            if (!is_match_index_column_name(table_schema, *index_schema, index_column_name)) {
              // skip
            } else if (!delta_buffer_table_is_valid) {
              is_hnsw = true;
              delta_buffer_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
            }
          } else if (index_schema->is_vec_index_id_type()) {
            if (!is_match_index_column_name(table_schema, *index_schema, index_column_name)) {
              // skip
            } else if (!index_id_table_is_valid) {
              is_hnsw = true;
              index_id_table_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
            }
          } else if (index_schema->is_vec_index_snapshot_data_type()) {
            if (!is_match_index_column_name(table_schema, *index_schema, index_column_name)) {
              // skip
            } else if (!snapshot_table_is_valid) {
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
        } else if (index_schema->is_vec_spiv_index()) {
          if (index_schema->is_vec_spiv_index_aux()) {
            if (!is_match_index_column_name(table_schema, *index_schema, index_column_name)) {
              // skip
            } else {
              is_spiv = true;
              spiv_dim_docid_value_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
            }
          } else if (index_schema->is_rowkey_doc_id()) {
            spiv_rowkey_docid_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
          } else if (index_schema->is_doc_id_rowkey()) {
            spiv_docid_rowkey_is_valid = index_schema->can_read_index() && index_schema->is_index_visible();
          }
        }
      }

      if(OB_FAIL(ret)){
      } else if ((is_hnsw && (is_ivf || is_spiv)) || (is_ivf && (is_hnsw || is_spiv))) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("only one vector index can be created on a vector column.", K(ret),
                  K(index_column_name), K(data_table_id), K(tenant_id), K(database_id));
      } else if (is_hnsw) {
        ObDocIDType vid_type = ObDocIDType::INVALID;
        if (OB_FAIL(ObVectorIndexUtil::determine_vid_type(table_schema, vid_type))) {
          LOG_WARN("Failed to check skip rowkey doc mapping", K(ret));
        } else if (vid_type == ObDocIDType::TABLET_SEQUENCE) {
          is_valid = rowkey_vid_table_is_valid && vid_rowkey_table_is_valid &&
                     delta_buffer_table_is_valid && index_id_table_is_valid && snapshot_table_is_valid;
        } else if (vid_type == ObDocIDType::HIDDEN_INC_PK) {
          is_valid = delta_buffer_table_is_valid && index_id_table_is_valid && snapshot_table_is_valid;
        }
        if (!is_valid) {
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
      } else if (is_spiv) {
        ObDocIDType docid_type = ObDocIDType::INVALID;
        if (OB_FAIL(ObFtsIndexBuilderUtil::determine_docid_type(table_schema, docid_type))) {
          LOG_WARN("Failed to check skip rowkey doc mapping", K(ret));
        } else if (docid_type == ObDocIDType::TABLET_SEQUENCE) {
          is_valid = spiv_dim_docid_value_is_valid && spiv_rowkey_docid_is_valid && spiv_docid_rowkey_is_valid;
        } else if (docid_type == ObDocIDType::HIDDEN_INC_PK) {
          is_valid = spiv_dim_docid_value_is_valid;
        }
        if (!is_valid) {
          LOG_WARN("spiv index is not all valid",
                    K(spiv_dim_docid_value_is_valid),
                    K(spiv_rowkey_docid_is_valid),
                    K(spiv_docid_rowkey_is_valid));
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
  } else if (index_table_schema.is_rowkey_doc_id() || index_table_schema.is_doc_id_rowkey()) {
    // skip rowkey_docid and docid_rowkey
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
        }
        // else if (!ori_col_schema->is_vec_hnsw_vector_column() && !ori_col_schema->is_vec_ivf_center_vector_column() && !ori_col_schema->is_vec_spiv_vec_column()) {
        //   // only need vec_vector column, here skip other column
        // }
        else if (OB_FAIL(ori_col_schema->get_cascaded_column_ids(cascaded_column_ids))) {
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

int ObVectorIndexUtil::check_drop_vec_indexs_ith_valid(
    const share::schema::ObTableSchema &index_schema, const int64_t schema_count,
    int64_t &rowkey_vid_ith, int64_t &vid_rowkey_ith, int64_t &domain_index_ith, int64_t &index_id_ith, int64_t &snapshot_data_ith,
    int64_t &centroid_ith, int64_t &cid_vector_ith, int64_t &rowkey_cid_ith, int64_t &sq_meta_ith, int64_t &pq_centroid_ith, int64_t &pq_code_ith)
{
  int ret = OB_SUCCESS;
  const ObIndexType index_type = index_schema.get_index_type();
  uint64_t vid_col_id = OB_INVALID_ID;
  bool has_vid_col = true;

  if (OB_FAIL(index_schema.get_vec_index_vid_col_id(vid_col_id, false))) {
    if (OB_ERR_INDEX_KEY_NOT_FOUND == ret) {
      has_vid_col = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get docid col id", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (share::schema::is_vec_hnsw_index(index_type)) {
    if (has_vid_col && (rowkey_vid_ith < 0 || rowkey_vid_ith >= schema_count ||
                        vid_rowkey_ith < 0 || vid_rowkey_ith >= schema_count)) {
      LOG_WARN("check drop vec hnsw index fail", K(ret), K(rowkey_vid_ith), K(vid_rowkey_ith));
    } else if (domain_index_ith < 0 || domain_index_ith >= schema_count ||
               index_id_ith < 0 || index_id_ith >= schema_count ||
               snapshot_data_ith < 0 || snapshot_data_ith >= schema_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("check drop vec hnsw index fail", K(ret), K(domain_index_ith), K(index_id_ith), K(snapshot_data_ith));
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
  int dim,
  const float *vector,
  const float *center_vec,
  float *residual)
{
int ret = OB_SUCCESS;
if (OB_ISNULL(residual)) {
  ret = OB_ERR_UNEXPECTED;
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
  } else {
    float *norm_vector = nullptr;
    if (OB_NOT_NULL(norm_info)) {
      if (OB_ISNULL(norm_vector = static_cast<float*>(alloc.alloc(dim * sizeof(float))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc norm vector", K(ret));
      } else if (FALSE_IT(MEMSET(norm_vector, 0, dim * sizeof(float)))) {
      } else if (OB_FAIL(norm_info->normalize_func_(dim, vector, norm_vector, nullptr))) {
        LOG_WARN("failed to normalize vector", K(ret));
      }
    }
    float *data = norm_vector == nullptr ? vector : norm_vector;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(calc_residual_vector(alloc, dim, data, center_vec, residual))) {
      LOG_WARN("fail to calc residual vector", K(ret), K(dim));
    }
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

int ObVectorIndexUtil::estimate_hnsw_memory(uint64_t num_vectors,
                                            const ObVectorIndexParam &param,
                                            uint64_t &est_mem,
                                            bool is_build)
{
  int ret = OB_SUCCESS;
  est_mem = 0;
  obvsag::VectorIndexPtr index_handler = nullptr;
  const char* const DATATYPE_FLOAT32 = "float32";
  ObVectorIndexAlgorithmType build_type = param.type_;
  int64_t build_metric = param.m_;
  build_metric = build_type == VIAT_HNSW_SQ ? get_hnswsq_type_metric(param.m_) : param.m_;
  if (param.type_ != VIAT_HNSW &&
      param.type_ != VIAT_HNSW_SQ &&
      param.type_ != VIAT_HNSW_BQ &&
      param.type_ != VIAT_HGRAPH) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hnsw algorithm type", K(ret), K(param));
  } else if (VIAT_HNSW == build_type && OB_FALSE_IT(build_type = VIAT_HGRAPH)) { // vsag not support hnsw estimate now, use hgraph
  } else if (OB_FAIL(obvectorutil::create_index(index_handler,
                                                build_type,
                                                DATATYPE_FLOAT32,
                                                VEC_INDEX_ALGTH[param.dist_algorithm_],
                                                param.dim_,
                                                build_metric,
                                                param.ef_construction_,
                                                param.ef_search_,
                                                nullptr, /* memory ctx, use default */
                                                param.extra_info_actual_size_,
                                                param.refine_type_,
                                                param.bq_bits_query_,
                                                param.bq_use_fht_))) {
    LOG_WARN("failed to create vsag index.", K(ret), K(param));
  } else if (OB_ISNULL(index_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(index_handler));
  } else if (OB_FALSE_IT(est_mem = obvectorutil::estimate_memory(index_handler, num_vectors, is_build))) {
  } else if (OB_FALSE_IT(obvectorutil::delete_index(index_handler))) {
  }
  return ret;
}

int ObVectorIndexUtil::estimate_ivf_memory(uint64_t num_vectors,
                                           const ObVectorIndexParam &param,
                                           uint64_t &construct_mem,
                                           uint64_t &buff_mem)
{
  int ret = OB_SUCCESS;
  int64_t nlist = MIN(num_vectors, param.nlist_);
  uint64_t sample_cnt = MIN(num_vectors, param.sample_per_nlist_ * nlist);
  if (param.type_ == VIAT_IVF_SQ8 || param.type_ == VIAT_IVF_FLAT) {
    buff_mem = sizeof(float) * nlist * param.dim_;
    construct_mem = 1000 + 2 * nlist * (nlist + 1) + 8 * nlist * param.dim_ + 4 * sample_cnt * (param.dim_ + 2);
  } else if (param.type_ == VIAT_IVF_PQ) {
    uint64_t ksub = MIN(num_vectors, 1L << param.nbits_);
    uint64_t pq_sample_cnt = MIN(num_vectors, ksub * param.sample_per_nlist_);
    buff_mem = sizeof(float) * param.dim_ * (ksub + nlist) + sizeof(float) * nlist * ksub * param.m_;
    uint64_t ivf_construct = 1000 + 2 * nlist * (nlist + 1) + 8 * nlist * param.dim_ + 4 * sample_cnt * (param.dim_ + 2);
    uint64_t pq_construct = 1000 + 4 * pq_sample_cnt * (param.dim_ + 1); // sample memused
    uint64_t pq_kmeans_mem = 0;
    if (OB_UNLIKELY(OB_FAIL(estimate_ivf_pq_kmeans_memory(num_vectors, param, 1 /*thread_cnt*/, pq_kmeans_mem)))) {
      LOG_WARN("failed to estimate ivf pq kmeans memory", K(ret));
    } else {
      pq_construct += pq_kmeans_mem;
      construct_mem = MAX(ivf_construct, pq_construct);
      buff_mem = sizeof(float) * param.dim_ * (ksub + nlist);
      if (param.dist_algorithm_ == VIDA_L2) {
        buff_mem += sizeof(float) * nlist * ksub * param.m_;
      }
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf algorithm type", K(ret), K(param));
  }
  construct_mem = static_cast<uint64_t>(construct_mem * 1.2);
  buff_mem = static_cast<uint64_t>(buff_mem * 1.2);
  return ret;
}

int ObVectorIndexUtil::estimate_ivf_pq_kmeans_memory(uint64_t num_vectors, const ObVectorIndexParam &param,
                                                     int64_t thread_cnt, uint64_t &kmeans_mem)
{
  int ret = OB_SUCCESS;
  if (param.type_ == VIAT_IVF_PQ) {
    int64_t pq_dim = param.dim_;
    if (param.m_ != 0) {
      pq_dim = param.dim_ / param.m_;
    }

    uint64_t ksub = MIN(num_vectors, 1L << param.nbits_);
    uint64_t pq_sample_cnt = MIN(num_vectors, ksub * param.sample_per_nlist_);
    kmeans_mem = (2 * ksub * (ksub + 1) + 4 * pq_sample_cnt + 8 * ksub * pq_dim) * thread_cnt;  // thread cnt is 1
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf algorithm type", K(ret), K(param));
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

int ObVectorIndexUtil::get_vector_domain_index_type(
      share::schema::ObSchemaGetterGuard *schema_guard,
      const ObTableSchema &data_table_schema,
      const int64_t col_id, // index col id
      ObIndexType &index_type)
{
  int ret = OB_SUCCESS;

  ObSEArray<ObAuxTableMetaInfo, 16> simple_index_infos;
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
    } else if (!index_table_schema->is_vec_domain_index()) {
      // skip none vector domain index
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

int ObVectorIndexUtil::split_vector(
  int pq_m,
  int dim,
  float* vector,
  ObIArray<float*> &splited_arrs)
{
  int ret = OB_SUCCESS;
  int64_t sub_dim = dim / pq_m;
  if (OB_ISNULL(vector)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector is null", K(ret), KP(vector));
  }
  for (int i = 0; OB_SUCC(ret) && i < pq_m; ++i) {
    float *splited_vec = vector + i * sub_dim;
    if (OB_FAIL(splited_arrs.push_back(splited_vec))) {
      SHARE_LOG(WARN, "failed to push back array", K(ret), K(i));
    }
  }
  return ret;
}

bool ObVectorIndexUtil::check_vector_index_memory(
    ObSchemaGetterGuard &schema_guard, const ObTableSchema &index_schema, const uint64_t tenant_id, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  bool is_satisfied = true;
  const static double VEC_MEMORY_HOLD_FACTOR = 1.2;
  MTL_SWITCH(tenant_id) {
    ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService*);
    ObSharedMemAllocMgr *shared_mem_mgr = MTL(ObSharedMemAllocMgr*);
    if (OB_ISNULL(service) || OB_ISNULL(shared_mem_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("service or manager is nullptr", K(ret), K(service), K(shared_mem_mgr));
    } else {
      ObRbMemMgr *mem_mgr = nullptr;
      int64_t bitmap_mem_used = 0;
      int64_t mem_limited_size = 0;
      int64_t estimate_memory = 0;
      int64_t all_vsag_mem_used = ATOMIC_LOAD(service->get_all_vsag_use_mem());
      int64_t hold_mem = shared_mem_mgr->vector_allocator().hold();
      if (OB_ISNULL(mem_mgr = MTL(ObRbMemMgr *))) {
      } else {
        bitmap_mem_used = mem_mgr->get_vec_idx_used();
      }
      if (OB_FAIL(ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id, mem_limited_size))) {
        LOG_WARN("failed to get vector mem limit size.", K(ret), K(tenant_id));
      } else if (OB_FAIL(estimate_vector_memory_used(schema_guard, index_schema, tenant_id, row_count, estimate_memory))) {
        LOG_WARN("failed to estimate vector memory used", K(ret), K(index_schema), K(row_count));
      } else if (OB_FALSE_IT(estimate_memory = ceil(estimate_memory * VEC_ESTIMATE_MEMORY_FACTOR * VEC_MEMORY_HOLD_FACTOR))) { // multiple 2.0， and need to consider the hold memory.
      } else if (hold_mem + estimate_memory > mem_limited_size) {
        is_satisfied = false;
      }
      LOG_INFO("finish estimate size", K(ret), K(is_satisfied),
        K(index_schema.get_table_name_str()), K(row_count), K(mem_limited_size), K(all_vsag_mem_used), K(hold_mem), K(bitmap_mem_used), K(estimate_memory));
    }
  }

  return is_satisfied;
}

bool ObVectorIndexUtil::check_ivf_vector_index_memory(ObSchemaGetterGuard &schema_guard, const uint64_t tenant_id, const ObTableSchema &index_schema, const int64_t row_count)
{
  int ret = OB_SUCCESS;
  bool is_satisfied = true;
  uint64_t construct_mem = 0;
  uint64_t buff_mem = 0;
  int64_t mem_limited_size = 0;
  ObSharedMemAllocMgr *shared_mem_mgr = MTL(ObSharedMemAllocMgr*);
  const ObTableSchema *data_table_schema = nullptr;
  ObVectorIndexParam param;
  int64_t dim = 0;
  ObSEArray<uint64_t , 1> col_ids;
  bool param_filled = false;
  ObVectorIndexType index_type = ObVectorIndexType::VIT_IVF_INDEX;
  const uint64_t data_table_id = index_schema.get_data_table_id();
  if (row_count <= 0) {
  } else if (!index_schema.is_vec_ivfpq_pq_centroid_index() && !index_schema.is_vec_ivf_centroid_index()) {
  } else if (OB_NOT_NULL(shared_mem_mgr)) {
    int64_t hold_mem = shared_mem_mgr->vector_allocator().hold();
    if (tenant_id == OB_INVALID_TENANT_ID || data_table_id == OB_INVALID_ID) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument, skip estimated", K(ret), K(tenant_id), K(data_table_id));
    } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(index_schema, dim))) {
      LOG_WARN("failed to get vec_index_col_param", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("failed to get table schema", K(ret));
    } else if (OB_ISNULL(data_table_schema) || data_table_schema->is_in_recyclebin()) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(ret), K(tenant_id), K(data_table_id), K(data_table_schema));
    } else if OB_FAIL(get_vector_index_column_id(*data_table_schema, index_schema, col_ids)) {
      LOG_WARN("failed to get vector index column id", K(ret), K(index_schema));
    } else if (col_ids.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid col id array", K(ret), K(col_ids));
    } else if (OB_FAIL(get_vector_index_param(&schema_guard, *data_table_schema, col_ids.at(0), param, param_filled))) {
      LOG_WARN("failed to get vector index param", K(ret), K(col_ids.at(0)));
    } else if (!param_filled) {
      LOG_INFO("skip esitmate memory", K(ret), K(param_filled));
    } else if (OB_FAIL(ObPluginVectorIndexHelper::get_vector_memory_limit_size(tenant_id, mem_limited_size))) {
      LOG_WARN("failed to get vector mem limit size.", K(ret), K(tenant_id));
    } else if (OB_FAIL(estimate_ivf_memory(row_count, param, construct_mem, buff_mem))) {
      LOG_WARN("failed to estimate ivf memory", K(ret));
    } else if (construct_mem + hold_mem > mem_limited_size) {
      is_satisfied = false;
    }
  }
  return is_satisfied;
}

// one tablet one vsag instance
int ObVectorIndexUtil::estimate_vector_memory_used(
    ObSchemaGetterGuard &schema_guard, const ObTableSchema &index_schema, const uint64_t tenant_id, const int64_t row_count, int64_t &estimate_memory)
{
  int ret = OB_SUCCESS;
  estimate_memory = 0;

  const char* const DATATYPE_FLOAT32 = "float32";
  obvsag::VectorIndexPtr index_handler = nullptr;
  ObVectorIndexParam param;
  int64_t dim = 0;
  ObSEArray<uint64_t , 1> col_ids;
  const ObTableSchema *data_table_schema = nullptr;
  const uint64_t data_table_id = index_schema.get_data_table_id();
  bool need_estimate = true;
  bool param_filled = false;

  // get index schema param
  if (!index_schema.is_vec_index_snapshot_data_type() || row_count <= 0) {
    need_estimate = false;
    LOG_INFO("target table is not table 5 or row_count <= 0, skip estimated",
      K(ret), K(index_schema), K(row_count));
  } else if (tenant_id == OB_INVALID_TENANT_ID || data_table_id == OB_INVALID_ID) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, skip estimated", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(index_schema, dim))) {
    LOG_WARN("failed to get vec_index_col_param", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
    LOG_WARN("failed to get table schema", K(ret));
  } else if (OB_ISNULL(data_table_schema) || data_table_schema->is_in_recyclebin()) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("table not exist", K(ret), K(tenant_id), K(data_table_id), K(data_table_schema));
  } else if OB_FAIL(get_vector_index_column_id(*data_table_schema, index_schema, col_ids)) {
    LOG_WARN("failed to get vector index column id", K(ret), K(index_schema));
  } else if (col_ids.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid col id array", K(ret), K(col_ids));
  } else if (OB_FAIL(get_vector_index_param(&schema_guard, *data_table_schema, col_ids.at(0), param, param_filled))) {
    LOG_WARN("failed to get vector index param", K(ret), K(col_ids.at(0)));
  }

  if (OB_FAIL(ret) || !param_filled) {
    LOG_INFO("skip esitmate memory", K(ret), K(param_filled));
  } else if (need_estimate) {
    ObVectorIndexAlgorithmType build_type = param.type_;
    int64_t build_metric = param.m_;
    param.dim_ = dim;
    build_metric = param.type_ == VIAT_HNSW_SQ ? get_hnswsq_type_metric(param.m_) : param.m_;
    if (OB_FAIL(obvectorutil::create_index(index_handler,
                                           build_type,
                                           DATATYPE_FLOAT32,
                                           VEC_INDEX_ALGTH[param.dist_algorithm_],
                                           param.dim_,
                                           build_metric,
                                           param.ef_construction_,
                                           param.ef_search_,
                                           nullptr, /* memory ctx, use default */
                                           param.extra_info_actual_size_,
                                           param.refine_type_,
                                           param.bq_bits_query_,
                                           param.bq_use_fht_))) {
      LOG_WARN("failed to create vsag index.", K(ret), K(param));
    } else if (OB_ISNULL(index_handler)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), KP(index_handler));
    } else if (OB_FALSE_IT(estimate_memory = obvectorutil::estimate_memory(index_handler, row_count, true/*is_build*/))) {
    } else if (OB_FALSE_IT(obvectorutil::delete_index(index_handler))) {
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("estimate vector index memory used.", K(estimate_memory), K(index_schema.get_table_name_str()), K(row_count), K(param));
  }
  return ret;
}

int ObVecExtraInfoPtr::init(ObIAllocator *allocator, const char *src_buf, int64_t extra_info_actual_size, int64_t count)
{
  int ret = OB_SUCCESS;
  char *alloc_buf = nullptr;
  if (OB_ISNULL(allocator) || count <= 0 || OB_ISNULL(src_buf) || extra_info_actual_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret), K(allocator), K(count), KP(src_buf), K(extra_info_actual_size));
  } else if (OB_ISNULL(alloc_buf = static_cast<char *>(allocator->alloc(sizeof(char *) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for vector index param", K(ret), K(count));
  } else if (OB_FALSE_IT(buf_ = new (alloc_buf) const char *[count])) {
  } else {
    extra_info_actual_size_ = extra_info_actual_size;
    count_ = count;
    for (int64_t i = 0; i < count && OB_SUCC(ret); ++i) {
      if (OB_ISNULL(src_buf + i * extra_info_actual_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid argument", K(ret), K(i), KP(src_buf));
      } else {
        buf_[i] = src_buf + i * extra_info_actual_size;
      }
    }
  }

  return ret;
}

int ObVecExtraInfoPtr::init(ObIAllocator *allocator, int64_t extra_info_actual_size, int64_t count)
{
  int ret = OB_SUCCESS;
  char *alloc_buf = nullptr;
  if (OB_ISNULL(allocator) || count <= 0 || extra_info_actual_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("allocator is null", K(ret), K(allocator), K(count), K(extra_info_actual_size));
  } else if (OB_ISNULL(alloc_buf = static_cast<char *>(allocator->alloc(sizeof(char *) * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for vector index param", K(ret), K(count));
  } else if (OB_FALSE_IT(buf_ = new (alloc_buf) const char *[count])) {
  } else {
    extra_info_actual_size_ = extra_info_actual_size;
    count_ = count;
    for (int64_t i = 0; i < count; ++i) {
      buf_[i] = nullptr;
    }
  }
  return ret;
}

int ObVectorIndexUtil::set_extra_info_actual_size_param(ObIAllocator *allocator, const ObString &old_param,
                                                        int64_t actual_size, ObString &new_param)
{
  int ret = OB_SUCCESS;
  char actual_extra_size_str[OB_MAX_TABLE_TYPE_LENGTH];
  int64_t pos = 0;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret), K(allocator));
  } else if (OB_FAIL(databuff_printf(actual_extra_size_str, OB_MAX_TABLE_TYPE_LENGTH, pos, ", EXTRA_INFO_ACTUAL_SIZE=%ld",
                              actual_size))) {
    LOG_WARN("fail to printf databuff", K(ret));
  } else {
    char *buf = nullptr;
    const int64_t alloc_len = old_param.length() + pos;
    if (OB_ISNULL(buf = (static_cast<char *>(allocator->alloc(alloc_len))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory for vector index param", K(ret), K(alloc_len));
    } else {
      MEMCPY(buf, old_param.ptr(), old_param.length());
      MEMCPY(buf + old_param.length(), actual_extra_size_str, pos);
      new_param.assign_ptr(buf, alloc_len);
      LOG_DEBUG("vector index params", K(new_param));
    }
  }
  return ret;
}

int ObVectorIndexUtil::alter_vec_aux_column_schema(const ObTableSchema &aux_table_schema,
                                                   const ObColumnSchemaV2 &new_column_schema,
                                                   ObColumnSchemaV2 &new_aux_column_schema)
{
  int ret = OB_SUCCESS;
  if (aux_table_schema.is_vec_index_snapshot_data_type() || aux_table_schema.is_vec_ivfpq_pq_centroid_index()) {
    // extra_info column in snapshot table is null
    if (new_column_schema.get_rowkey_position() > 0 || new_column_schema.get_tbl_part_key_pos() > 0) {
      new_aux_column_schema.set_nullable(true);
      new_aux_column_schema.drop_not_null_cst();
    }
  }

  return ret;
}

int ObVecExtraInfo::extra_infos_to_buf(ObIAllocator &allocator, const ObVecExtraInfoObj *extra_info_objs,
                                       int64_t extra_column_count, int64_t extra_info_actual_size, int64_t count,
                                       char *&buf)
{
  int ret = OB_SUCCESS;
  char *begin_buf = nullptr;
  if (extra_info_actual_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(extra_info_actual_size));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(extra_info_actual_size * count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory for vector index param", K(ret), K(extra_info_actual_size), K(count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
      begin_buf = buf + i * extra_info_actual_size;
      int64_t pos = 0;
      const ObVecExtraInfoObj *extra_info_i = extra_info_objs + (i * extra_column_count);
      if (OB_ISNULL(extra_info_i)) {
        ret = OB_INVALID_DATA;
        LOG_WARN("extra_info_i is null", K(ret), KP(extra_info_i), K(i));
      } else if (OB_FAIL(extra_info_to_buf(extra_info_i, extra_column_count, begin_buf, extra_info_actual_size, pos))) {
        LOG_WARN("fail to serialize value", K(ret), K(extra_info_actual_size), K(count));
      }
    }
  }
  return ret;
}

int ObVecExtraInfo::extra_buf_to_obj(const char *buf, int64_t data_len, int64_t extra_column_count, ObObj *obj,
                                     const ObIArray<int64_t> *extra_in_rowkey_idxs_ /*nullptr*/)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_ISNULL(obj)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), KP(obj));
  } else if (extra_column_count <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(extra_column_count));
  } else if (OB_NOT_NULL(extra_in_rowkey_idxs_) && extra_in_rowkey_idxs_->count() != extra_column_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(extra_in_rowkey_idxs_->count()), K(extra_column_count));
  } else {
    int64_t pos = 0;
    uint32_t len = 0;
    int64_t version = 0;
    OB_UNIS_DECODE(version);
    if (OB_SUCC(ret)) {
      if (version != UNIS_VERSION) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("version not match", K(ret), K(version));
      }
    }
    for (int64_t i = 0; i < extra_column_count && OB_SUCC(ret); ++i) {
      int64_t real_idx = OB_ISNULL(extra_in_rowkey_idxs_) ? i : extra_in_rowkey_idxs_->at(i);
      if (real_idx >= extra_column_count || OB_ISNULL(obj + real_idx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("obj is null or real_idx invalid", K(ret), K(real_idx), K(extra_in_rowkey_idxs_));
      } else {
        common::ObObjDatumMapType obj_map_type = ObDatum::get_obj_datum_map_type(obj[real_idx].get_type());
        if (OB_UNLIKELY(!is_obj_type_supported(obj[real_idx].get_type()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("obj type not supported", K(ret), K(obj[real_idx].get_type()));
        } else if (obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_8BYTE_DATA ||
                   obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_4BYTE_DATA ||
                   obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_1BYTE_DATA) {
          len = ObDatum::get_reserved_size(obj_map_type);
          memcpy(&obj[real_idx].v_.uint64_, buf + pos, len);
          pos += len;
        } else if (obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_STRING) {
          len = *(int32_t *)(buf + pos);
          pos += sizeof(len);
          obj[real_idx].v_.string_ = buf + pos;
          obj[real_idx].val_len_ = len;
          pos += len;
        }
      }
    }
  }

  return ret;
}

int ObVecExtraInfo::extra_info_to_buf(const ObVecExtraInfoObj *extra_obj, int64_t extra_column_count, char *buf,
                                      const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(UNIS_VERSION);
  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(!is_legal(extra_obj, extra_column_count))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("illegal extra_info.", KP(extra_obj), K(extra_column_count), K(ret));
    } else {
      int32_t len = 0;
      for (int64_t i = 0; i < extra_column_count && OB_SUCCESS == ret; ++i) {
        if (OB_UNLIKELY(OB_ISNULL(extra_obj + i))) {
          ret = OB_INVALID_DATA;
          LOG_WARN("illegal extra_info.", KP(extra_obj), K(extra_column_count), K(ret));
        } else {
          common::ObObjDatumMapType obj_map_type = extra_obj[i].obj_map_type_;
          if (obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_8BYTE_DATA ||
              obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_4BYTE_DATA ||
              obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_1BYTE_DATA) {
            len = ObDatum::get_reserved_size(obj_map_type);
            memcpy(buf + pos, extra_obj[i].ptr_, len);
            pos += len;
          } else if (obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_STRING) {
            len = extra_obj[i].len_;
            memcpy(buf + pos, &extra_obj[i].len_, sizeof(len));
            pos += sizeof(len);
            memcpy(buf + pos, extra_obj[i].ptr_, len);
            pos += len;
          }
        }
      }
    }
  }

  return ret;
}

int64_t ObVecExtraInfo::get_to_buf_size(const ObVecExtraInfoObj *extra_obj, int64_t extra_column_count)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;

  OB_UNIS_ADD_LEN(UNIS_VERSION);
  if (OB_UNLIKELY(!is_legal(extra_obj, extra_column_count))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("illegal extra_info.", KP(extra_obj), K(extra_column_count), K(ret));
  } else if (extra_column_count > 0) {
    for (int64_t i = 0; i < extra_column_count && OB_SUCCESS == ret; ++i) {
      if (OB_UNLIKELY(OB_ISNULL(extra_obj + i))) {
        ret = OB_INVALID_DATA;
        LOG_WARN("illegal extra_info.", KP(extra_obj), K(extra_column_count), K(ret));
      } else {
        common::ObObjDatumMapType obj_map_type = extra_obj[i].obj_map_type_;
        if (obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_8BYTE_DATA ||
            obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_4BYTE_DATA ||
            obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_1BYTE_DATA) {
          len += ObDatum::get_reserved_size(obj_map_type);
        } else if (obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_STRING) {
          len += sizeof(extra_obj[i].len_);
          len += extra_obj[i].len_;
        }
      }
    }
  }

  return len;
}

int64_t ObVecExtraInfo::get_encode_size(const ObIArray<ObVecExtraInfoObj> &extra_obj)
{
  int64_t len = 0;
  int ret = OB_SUCCESS;
  OB_UNIS_ADD_LEN(UNIS_VERSION);
  int64_t extra_column_count = extra_obj.count();
  if (extra_column_count > 0) {
    for (int64_t i = 0; i < extra_column_count && OB_SUCCESS == ret; ++i) {
      common::ObObjDatumMapType obj_map_type = extra_obj.at(i).obj_map_type_;
      if (is_fixed_length_type(obj_map_type)) {
        len += ObDatum::get_reserved_size(obj_map_type);
      } else if (obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_STRING) {
        len += sizeof(extra_obj.at(i).len_);
        len += extra_obj.at(i).len_;
      }
    }
  }
  return len;
}

int ObVecExtraInfoObj::from_datum(const ObDatum &datum, const common::ObObjMeta &type, ObIAllocator *allocator /*nullptr*/)
{
  int ret = OB_SUCCESS;
  // Note: extra_info must not null
  if (OB_UNLIKELY(datum.is_null() || !ObVecExtraInfo::is_obj_type_supported(type.get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param, extra_info must not null, or type not supported", K(datum), K(type), K(ret));
  } else {
    const common::ObObjDatumMapType &obj_map_type = common::ObDatum::get_obj_datum_map_type(type.get_type());
    if (allocator == nullptr) {
      if (ObVecExtraInfo::is_fixed_length_type(obj_map_type)) {
        ptr_ = datum.ptr_;
        len_ = ObDatum::get_reserved_size(obj_map_type);
        obj_map_type_ = obj_map_type;
      } else if (obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_STRING) {
        ptr_ = datum.ptr_;
        len_ = datum.len_;
        obj_map_type_ = obj_map_type;
      }
    } else {
      if (ObVecExtraInfo::is_fixed_length_type(obj_map_type)) {
        len_ = ObDatum::get_reserved_size(obj_map_type);
      } else if (obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_STRING) {
        len_ = datum.len_;
      }
      char *buf = nullptr;
      if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(len_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(ret));
      } else {
        MEMCPY(buf, datum.ptr_, len_);
        ptr_ = buf;
        obj_map_type_ = obj_map_type;
      }
    }
  }
  return ret;
}

int ObVecExtraInfoObj::from_obj(const ObObj &obj, ObIAllocator *allocator /*nullptr*/)
{
  int ret = OB_SUCCESS;
  // Note: extra_info must not null
  if (OB_UNLIKELY(obj.is_null() || !ObVecExtraInfo::is_obj_type_supported(obj.get_type()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param, extra_info must not null, or type not supported", K(obj), K(ret));
  } else {
    const common::ObObjDatumMapType &obj_map_type = common::ObDatum::get_obj_datum_map_type(obj.get_type());
    if (allocator == nullptr) {
      if (ObVecExtraInfo::is_fixed_length_type(obj_map_type)) {
        ptr_ =  reinterpret_cast<const char *>(&obj.v_.uint64_);
        len_ = ObDatum::get_reserved_size(obj_map_type);
        obj_map_type_ = obj_map_type;
      } else if (obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_STRING) {
        ptr_ = obj.v_.string_;
        len_ = obj.val_len_ ;
        obj_map_type_ = obj_map_type;
      }
    } else {
      char *buf = nullptr;
      if (ObVecExtraInfo::is_fixed_length_type(obj_map_type)) {
        len_ = ObDatum::get_reserved_size(obj_map_type);
        if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(len_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        } else {
          MEMCPY(buf, &obj.v_.uint64_, len_);
          ptr_ = buf;
          obj_map_type_ = obj_map_type;
        }
      } else if (obj_map_type == common::ObObjDatumMapType::OBJ_DATUM_STRING) {
        len_ = obj.val_len_ ;
        if (OB_ISNULL(buf = static_cast<char *>(allocator->alloc(len_)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", K(ret));
        } else {
          MEMCPY(buf, obj.v_.string_, len_);
          ptr_ = buf;
          obj_map_type_ = obj_map_type;
        }
      }
    }
  }

  return ret;
}

// when change search params and same build params, only change table schema
int ObVectorIndexUtil::check_only_change_search_params(const ObString &old_idx_params,
                                                       const ObString &new_idx_params,
                                                       const ObTableSchema &index_table_schema,
                                                       bool &only_change_search_params)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam old_vector_index_param;
  ObVectorIndexParam new_vector_index_param;
  only_change_search_params = false;
  ObVectorIndexType index_type = ObVectorIndexType::VIT_MAX;
  if (index_table_schema.is_vec_hnsw_index()) {
    index_type = ObVectorIndexType::VIT_HNSW_INDEX;
    if (OB_FAIL(parser_params_from_string(old_idx_params, index_type, old_vector_index_param))) {
      LOG_WARN("fail to parser params from string", K(ret), K(old_idx_params));
    } else if (OB_FAIL(parser_params_from_string(new_idx_params, index_type, new_vector_index_param))) {
      LOG_WARN("fail to parser params from string", K(ret), K(new_idx_params));
    } else {
      if (old_vector_index_param.type_ != new_vector_index_param.type_ ||
          old_vector_index_param.lib_ != new_vector_index_param.lib_ ||
          old_vector_index_param.dist_algorithm_ != new_vector_index_param.dist_algorithm_ ||
          old_vector_index_param.dim_ != new_vector_index_param.dim_ ||
          old_vector_index_param.m_ != new_vector_index_param.m_ ||
          old_vector_index_param.ef_construction_ != new_vector_index_param.ef_construction_ ||
          old_vector_index_param.extra_info_max_size_ != new_vector_index_param.extra_info_max_size_ ||
          old_vector_index_param.extra_info_actual_size_ != new_vector_index_param.extra_info_actual_size_) {
        only_change_search_params = false;
      } else if (old_vector_index_param.ef_search_ != new_vector_index_param.ef_search_) {
        only_change_search_params = true;
      }
    }
  } else {
    // do nothing, other type vec index.
  }
  return ret;
}

int ObVectorIndexUtil::set_vector_index_param(const ObTableSchema *&vec_index_schema,
                                              ObVecIdxExtraInfo &vec_extra_info,
                                              double &selectivity,
                                              sql::ObRawExpr *&vector_expr,
                                              const sql::ObDMLStmt *&stmt)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vec_extra_info.set_vec_param_info(vec_index_schema))) {
    LOG_WARN("fail to set vector param info", K(ret), K(vec_index_schema));
  } else {
    vec_extra_info.set_selectivity(selectivity);
    // for optimize, distance expr just for order by needn't calculate
    // using vsag calc result is ok
    if (OB_ISNULL(vector_expr)) {
      vector_expr = stmt->get_first_vector_expr();
    }
    if (OB_NOT_NULL(vector_expr) &&
        vec_extra_info.is_hnsw_vec_scan()
        && ! vec_extra_info.is_hnsw_bq_scan()
        &&!stmt->is_contain_vector_origin_distance_calc()) {
      FLOG_INFO("distance needn't calc", K(ret));
      vector_expr->add_flag(IS_CUT_CALC_EXPR);
    }
  }
  return ret;
}

int ObVectorIndexUtil::set_adaptive_try_path(ObVecIdxExtraInfo& vc_info, const bool is_primary_idx)
{
  int ret = OB_SUCCESS;
  double output_row_count = vc_info.row_count_ * vc_info.selectivity_;
  if (vc_info.adaptive_try_path_ == ObVecIdxAdaTryPath::VEC_PATH_UNCHOSEN) {
    if (output_row_count <= ObVecIdxExtraInfo::MAX_HNSW_BRUTE_FORCE_SIZE) {
      vc_info.adaptive_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER;
    } else if (is_primary_idx) {
      vc_info.adaptive_try_path_ = (output_row_count < ObVecIdxExtraInfo::MAX_HNSW_PRE_ROW_CNT_WITH_ROWKEY
                                    && vc_info.selectivity_ <= ObVecIdxExtraInfo::DEFAULT_PRE_RATE_FILTER_WITH_ROWKEY) ?
                                    ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER :  ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
    } else {
      vc_info.adaptive_try_path_ = (output_row_count < ObVecIdxExtraInfo::MAX_HNSW_PRE_ROW_CNT_WITH_IDX
                                    && vc_info.selectivity_ <= ObVecIdxExtraInfo::DEFAULT_PRE_RATE_FILTER_WITH_IDX) ?
                                    ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER : ObVecIdxAdaTryPath::VEC_INDEX_ITERATIVE_FILTER;
    }
  } else if (vc_info.adaptive_try_path_ == ObVecIdxAdaTryPath::VEC_INDEX_PRE_FILTER) {
    // means hint choose pre-filter, only check can/can't go in-filter
    if (vc_info.with_extra_info_ && vc_info.can_use_vec_pri_opt_) {
      vc_info.adaptive_try_path_ = ObVecIdxAdaTryPath::VEC_INDEX_IN_FILTER;
    }
  }
  return ret;
}

int ObVecIdxExtraInfo::set_vec_param_info(const ObTableSchema *vec_index_schema)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vec_index_schema)) {
    ret = OB_BAD_NULL_ERROR;
  } else {
    ObVectorIndexType vec_type = ObVectorIndexType::VIT_MAX;
    switch (vec_index_schema->get_index_type()) {
      case ObIndexType::INDEX_TYPE_VEC_ROWKEY_VID_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_VID_ROWKEY_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_INDEX_ID_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL: {
        vector_index_param_.type_ = ObVectorIndexAlgorithmType::VIAT_HNSW;
        vec_type = ObVectorIndexType::VIT_HNSW_INDEX;
        break;
      }
      case ObIndexType::INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_IVFFLAT_CID_VECTOR_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_IVFFLAT_ROWKEY_CID_LOCAL: {
        vector_index_param_.type_  = ObVectorIndexAlgorithmType::VIAT_IVF_FLAT;
        vec_type = ObVectorIndexType::VIT_IVF_INDEX;
        break;
      }
      case ObIndexType::INDEX_TYPE_VEC_IVFSQ8_CENTROID_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_IVFSQ8_META_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_IVFSQ8_CID_VECTOR_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_IVFSQ8_ROWKEY_CID_LOCAL: {
        vector_index_param_.type_  = ObVectorIndexAlgorithmType::VIAT_IVF_SQ8;
        vec_type = ObVectorIndexType::VIT_IVF_INDEX;
        break;
      }
      case ObIndexType::INDEX_TYPE_VEC_IVFPQ_CENTROID_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_IVFPQ_CODE_LOCAL:
      case ObIndexType::INDEX_TYPE_VEC_IVFPQ_ROWKEY_CID_LOCAL: {
        vector_index_param_.type_  = ObVectorIndexAlgorithmType::VIAT_IVF_PQ;
        vec_type = ObVectorIndexType::VIT_IVF_INDEX;
        break;
      }
      case ObIndexType::INDEX_TYPE_VEC_SPIV_DIM_DOCID_VALUE_LOCAL: {
        vector_index_param_.type_  = ObVectorIndexAlgorithmType::VIAT_SPIV;
        vec_type = ObVectorIndexType::VIT_SPIV_INDEX;
        break;
      }
      default: {
        vector_index_param_.type_ = ObVectorIndexAlgorithmType::VIAT_MAX;
        break;
      }
    }

    if (vector_index_param_.type_  == ObVectorIndexAlgorithmType::VIAT_MAX) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid vector index type", K(ret), K(vector_index_param_.type_));
    } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(vec_index_schema->get_index_params(), vec_type, vector_index_param_))) {
      LOG_WARN("fail to parser params from string", K(ret), K(vec_index_schema->get_index_params()));
    } else {
      with_extra_info_ = vector_index_param_.extra_info_max_size_ > 0;
    }
  }
  return ret;
}
}
}
