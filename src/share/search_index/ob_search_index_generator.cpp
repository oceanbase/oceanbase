/**
 * Copyright (c) 2024 OceanBase
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

#include "share/search_index/ob_search_index_generator.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_tree.h"
#include "lib/hash/ob_hashset.h"
#include "lib/json_type/ob_json_bin.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/number/ob_number_v2.h"
#include "share/vector/ob_fixed_length_base.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "lib/udt/ob_collection_type.h"

namespace oceanbase
{
namespace share
{


int ObSearchIndexRow::build_datum_row(ObIAllocator &allocator, blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(datum_row.init(allocator, SEARCH_INDEX_KEY_CNT))) {
    LOG_WARN("fail to init datum row", K(ret));
  } else {
    datum_row.storage_datums_[SEARCH_INDEX_COL_IDX].set_uint(column_idx_);
    datum_row.storage_datums_[SEARCH_INDEX_PATH].shallow_copy_from_datum(path_);
    datum_row.storage_datums_[SEARCH_INDEX_VALUE].shallow_copy_from_datum(value_);
    datum_row.storage_datums_[SEARCH_INDEX_DOC_ID].set_uint(doc_id_);
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::init(const uint64_t column_idx, const ObObjMeta &obj_meta,
                                               const common::ObCollectionArrayType *arr_type)
{
  int ret = OB_SUCCESS;
  column_idx_ = column_idx;
  obj_meta_ = obj_meta;
  const ObObjType col_type = obj_meta_.get_type();
  if (ob_is_json(col_type)) {
    if (OB_FAIL(ObSearchIndexValueEncoder::init_json_string_enc_param(enc_param_))) {
      LOG_WARN("fail to init json string enc param", K(ret));
    }
  } else if (ob_is_string_tc(col_type)) {
    if (OB_FAIL(ObSearchIndexValueEncoder::init_string_enc_param(obj_meta_.get_type(),
                                                                 obj_meta_.get_collation_type(),
                                                                 0, /** sql_mode */
                                                                 true, /** is_null_first */
                                                                 true, /** is_asc */
                                                                 enc_param_))) {
      LOG_WARN("fail to init string enc param", K(ret));
    }
  } else if (ObCollectionSQLType == col_type) {
    if (OB_FAIL(init_array_generator(arr_type))) {
      LOG_WARN("fail to init array generator", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::init_array_generator(const common::ObCollectionArrayType *arr_type)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arr_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("arr_type is null", K(ret));
  } else {
    // deep copy arr_type to inner_allocator_ so its lifetime is managed by generator
    common::ObCollectionTypeBase *dst = nullptr;
    if (OB_FAIL(arr_type->deep_copy(inner_allocator_, dst))) {
      LOG_WARN("fail to deep copy arr type", K(ret));
    } else {
      arr_type_ = static_cast<common::ObCollectionArrayType *>(dst);
      uint32_t depth = 0;
      const ObDataType *basic_elem_type = &(arr_type_->get_basic_meta(depth));
      ObObjMeta elem_meta = basic_elem_type->get_meta_type();
      void *elem_generator_ptr = nullptr;
      if (OB_ISNULL(elem_generator_ptr = inner_allocator_.alloc(sizeof(Generator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for element generator", K(ret));
      } else {
        elem_generator_ = new (elem_generator_ptr) Generator(allocator_, inner_allocator_);
        if (OB_FAIL(elem_generator_->init(column_idx_, elem_meta, nullptr))) {
          LOG_WARN("fail to init element generator", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::generate_rows(
    const int64_t doc_id, const ObDatum& datum,
    ObSearchIndexRows &rows)
{
  int ret = OB_SUCCESS;
  const ObObjType obj_type = obj_meta_.get_type();
  if (OB_UNLIKELY(datum.is_null())) {
    // datum null: column_idx, null, null, doc_id
    if (OB_FAIL(rows.push_back(ObSearchIndexRow(column_idx_, doc_id)))) {
      LOG_WARN("fail to push back gin row", K(ret));
    }
  } else if (ob_is_json(obj_type)) {
    if (OB_FAIL(generate_json_rows(doc_id, datum, rows))) {
      LOG_WARN("fail to generate json gin rows", K(ret));
    }
  } else if (ob_is_collection_sql_type(obj_type)) {
    if (OB_FAIL(generate_array_rows(doc_id, datum, rows))) {
      LOG_WARN("fail to generate array gin rows", K(ret));
    }
  } else {
    // scalar type, one datum generate one row
    ObString value;
    switch (obj_type) {
      case ObTinyIntType:
      case ObSmallIntType:
      case ObMediumIntType:
      case ObInt32Type:
      case ObIntType: {
        ret = ObSearchIndexValueEncoder::encode_int(allocator_, datum.get_int(), value);
        break;
      }
      case ObUTinyIntType:
      case ObUSmallIntType:
      case ObUMediumIntType:
      case ObUInt32Type:
      case ObUInt64Type: {
        ret = ObSearchIndexValueEncoder::encode_uint(allocator_, datum.get_uint(), value);
        break;
      }
      case ObFloatType:
      case ObUFloatType: {
        ret = ObSearchIndexValueEncoder::encode_float(allocator_, datum.get_float(), value);
        break;
      }
      case ObDoubleType:
      case ObUDoubleType: {
        ret = ObSearchIndexValueEncoder::encode_double(allocator_, datum.get_double(), value);
        break;
      }
      case ObVarcharType:
      case ObCharType: {
        ret = ObSearchIndexValueEncoder::encode_string(allocator_, datum.get_string(), value,
                                                       enc_param_);
        break;
      }
      case ObDateType:
      case ObMySQLDateType: {
        ret = ObSearchIndexValueEncoder::encode_int32(allocator_, datum.get_date(), value);
        break;
      }
      case ObDateTimeType:
      case ObTimestampType:
      case ObMySQLDateTimeType: {
        ret = ObSearchIndexValueEncoder::encode_int(allocator_, datum.get_datetime(), value);
        break;
      }
      case ObTimeType: {
        ret = ObSearchIndexValueEncoder::encode_int(allocator_, datum.get_time(), value);
        break;
      }
      case ObYearType: {
        ret = ObSearchIndexValueEncoder::encode_uint8(allocator_, datum.get_year(), value);
        break;
      }
      case ObNumberType:
      case ObUNumberType: {
        const number::ObNumber number(datum.get_number());
        ret = ObSearchIndexValueEncoder::encode_number(allocator_, number, value);
        break;
      }
      case ObDecimalIntType: {
        ret = ObSearchIndexValueEncoder::encode_decimal_int(allocator_, datum.get_decimal_int(),
                                                            datum.get_int_bytes(), value);
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unsupported object type", K(ret), K(obj_type));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(rows.push_back(ObSearchIndexRow(column_idx_, value, doc_id)))) {
        LOG_WARN("fail to push back gin row", K(ret));
      }
    }
  }
  return ret;
}


int ObSearchIndexRowGenerator::Generator::generate_rows(const ObIVector &doc_id_vec,
                                                        const ObIVector &data_vec,
                                                        const int64_t batch_size,
                                                        ObSearchIndexRows &rows)
{
  return OB_NOT_IMPLEMENT;
}

int ObSearchIndexRowGenerator::Generator::generate_json_rows(const int64_t doc_id,
                                                             const ObDatum& datum,
                                                             ObSearchIndexRows &rows)
{
  int ret = OB_SUCCESS;
  ObString j_bin_str = datum.get_string();
  ObIJsonBase *j_base = nullptr;
  ObSEArray<ObSearchIndexPathEncoder::JsonPathItem, 16> path_items;
  if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(
      &allocator_, ObJsonType, CS_TYPE_UTF8MB4_BIN, true, j_bin_str))) {
    LOG_WARN("fail to read real string data", K(ret));
  } else if (OB_FAIL(common::ObJsonBaseFactory::get_json_base(
                                                              &allocator_,
                                                              j_bin_str,
                                                              common::ObJsonInType::JSON_BIN,
                                                              common::ObJsonInType::JSON_BIN,
                                                              j_base))) {
    LOG_WARN("fail to get json base", K(ret));
  } else {
    // document-level (path, value) dedup to avoid duplicate key for nested array same values
    PathValueDedupSet path_value_dedup_set;
    if (OB_FAIL(path_value_dedup_set.create(256, ObMemAttr(MTL_ID(), "SeaIdxValDeDup")))) {
      LOG_WARN("fail to create path value dedup set", K(ret));
    } else if (OB_FAIL(generate_rows(doc_id, j_base, path_items, rows, path_value_dedup_set))) {
      LOG_WARN("failed to generate gin rows", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::generate_rows(
    const int64_t doc_id, ObIJsonBase *j_base,
    ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
    ObSearchIndexRows &rows,
    PathValueDedupSet &path_value_dedup_set)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(j_base)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("json base is null", K(ret));
  } else {
    ObJsonNodeType node_type = j_base->json_type();
    if (ObJsonNodeType::J_OBJECT == node_type) {
      JsonObjectIterator iter(j_base);
      while (OB_SUCC(ret) && !iter.end()) {
        common::ObJsonObjPair elem;
        if (OB_FAIL(iter.get_elem(elem))) {
          LOG_WARN("fail to get elem", K(ret));
        } else if (OB_FAIL(path_items.push_back(ObSearchIndexPathEncoder::make_object_path(elem.first)))) {
          LOG_WARN("fail to push path string", K(ret));
        } else if (OB_FAIL(generate_rows(doc_id, elem.second, path_items, rows, path_value_dedup_set))) {
          LOG_WARN("fail to generate gin rows", K(ret));
        } else {
          path_items.pop_back();
          iter.next();
        }
      }
    } else if (ObJsonNodeType::J_ARRAY == node_type) {
      const uint64_t elem_cnt = j_base->element_count();
      if (elem_cnt == 0) {
        // empty array, do nothing
      } else if (OB_FAIL(path_items.push_back(ObSearchIndexPathEncoder::make_array_path()))) {
        LOG_WARN("fail to push path string", K(ret));
      } else {
        for (uint64_t i = 0; OB_SUCC(ret) && i < elem_cnt; ++i) {
          ObIJsonBase *elem = nullptr;
          if (OB_FAIL(j_base->get_array_element(i, elem))) {
            LOG_WARN("fail to get array elem", K(ret), K(i), K(elem_cnt));
          } else if (OB_FAIL(generate_rows(doc_id, elem, path_items, rows, path_value_dedup_set))) {
            LOG_WARN("fail to generate gin rows for array elem", K(ret));
          }
        }
        path_items.pop_back();
      }
    } else {
      // scalar json node
      ObString path;
      ObString value;
      if (OB_FAIL(ObSearchIndexValueEncoder::encode_json_scalar(allocator_, j_base, value,
                                                                enc_param_))) {
        LOG_WARN("fail to encode json scalar", K(ret));
      } else if (OB_FAIL(ObSearchIndexPathEncoder::encode_path(allocator_, path_items, node_type,
                                                               path))) {
        LOG_WARN("fail to encode path", K(ret));
      } else {
        // document-level dedup: skip if same (path, value) already emitted (handles nested arrays)
        PathValueKey key;
        key.path_ = path;
        key.value_ = value;
        int tmp_ret = path_value_dedup_set.set_refactored(key, 0);
        if (tmp_ret == OB_HASH_EXIST) {
          // already emitted this (path, value), skip to avoid duplicate key
        } else if (tmp_ret != OB_SUCCESS) {
          ret = tmp_ret;
          LOG_WARN("fail to insert to path value dedup set", K(ret));
        } else if (OB_FAIL(rows.push_back(ObSearchIndexRow(column_idx_, path, value, doc_id)))) {
          LOG_WARN("fail to push back gin row", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::generate_rows_for_array_obj(
    const int64_t doc_id,
    common::ObIArrayType *arr_obj,
    ObSearchIndexRows &rows)
{
  int ret = OB_SUCCESS;
  common::ObIArrayType *distinct_arr_obj = nullptr;
  if (OB_ISNULL(arr_obj) || OB_ISNULL(elem_generator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(arr_obj), KP_(elem_generator));
  } else if (arr_obj->get_format() == ArrayFormat::Nested_Array) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("nested array is not supported for now", K(ret));
  } else if (OB_FAIL(arr_obj->distinct(allocator_, distinct_arr_obj))) {
    LOG_WARN("fail to distinct array", K(ret));
  } else if (OB_FAIL(distinct_arr_obj->init())) {
    LOG_WARN("array init failed", K(ret));
  } else {
    uint32_t len = distinct_arr_obj->size();
    for (uint32_t i = 0; OB_SUCC(ret) && i < len; ++i) {
      if (distinct_arr_obj->is_null(i)) {
        continue;
      }
      common::ObObj elem_obj;
      ObDatum elem_datum;
      char elem_datum_buf[common::OBJ_DATUM_MAX_RES_SIZE] = {0};
      elem_datum.ptr_ = elem_datum_buf;
      if (OB_FAIL(distinct_arr_obj->elem_at(i, elem_obj))) {
        LOG_WARN("fail to get array element", K(ret), K(i));
      } else if (OB_FAIL(elem_datum.from_obj(elem_obj))) {
        LOG_WARN("fail to convert obj to datum", K(ret), K(elem_obj));
      } else if (OB_FAIL(elem_generator_->generate_rows(doc_id, elem_datum, rows))) {
        LOG_WARN("fail to generate gin rows for array element", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::Generator::generate_array_rows(const int64_t doc_id,
                                                            const ObDatum& datum,
                                                            ObSearchIndexRows &rows)
{
  int ret = OB_SUCCESS;
  ObString data_str = datum.get_string();
  if (OB_ISNULL(arr_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arr_type is null", K(ret));
  } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator_,
                                      ObLongTextType,
                                      CS_TYPE_BINARY,
                                      true,
                                      data_str))) {
    LOG_WARN("fail to get real data.", K(ret), K(data_str));
  } else {
    uint32_t len = 0, data_len = 0;
    uint8_t *null_bitmaps = nullptr;
    const char *data = nullptr;
    common::ObIArrayType *arr_obj = nullptr;
    if (OB_FAIL(sql::ObArrayExprUtils::get_array_data(data_str,
                                        arr_type_,
                                        len,
                                        null_bitmaps,
                                        data,
                                        data_len))) {
      LOG_WARN("failed to get array data", K(ret));
    } else if (OB_FAIL(common::ObArrayTypeObjFactory::construct(allocator_, *arr_type_, arr_obj, true))) {
      LOG_WARN("fail to construct array obj", K(ret));
    } else if (OB_FAIL(arr_obj->init(data_str))) {
      LOG_WARN("fail to init array obj", K(ret));
    } else if (OB_FAIL(generate_rows_for_array_obj(doc_id, arr_obj, rows))) {
      LOG_WARN("fail to generate gin rows from array", K(ret));
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::init(const common::ObIArray<int32_t> &included_cid_idxes,
                                    const common::ObIArray<ObObjMeta> &included_obj_metas,
                                    const common::ObIArray<int64_t> &row_projector,
                                    const ObString &index_properties,
                                    bool rowkey_only,
                                    const common::ObIArray<common::ObCollectionArrayType*> *arr_types)
{
  int ret = OB_SUCCESS;
  UNUSED(rowkey_only);
  CK (included_cid_idxes.count() == included_obj_metas.count());
  void *mem = nullptr;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(row_projector_.assign(row_projector))) {
    LOG_WARN("failed to assign row projector", K(ret));
  } else if (OB_FAIL(generators_.prepare_allocate(included_cid_idxes.count()))) {
    LOG_WARN("fail to init generators", K(ret));
  } else if (OB_ISNULL(mem = inner_allocator_.alloc(sizeof(Generator) * included_cid_idxes.count()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", K(ret));
  } else if (OB_NOT_NULL(arr_types) && included_cid_idxes.count() != arr_types->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("included cid idxes count mismatch with arr types count", K(ret),
             K(included_cid_idxes.count()), K(arr_types->count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < included_cid_idxes.count(); ++i) {
      generators_.at(i) = new (static_cast<char*>(mem) + i * sizeof(Generator)) Generator(row_allocator_, inner_allocator_);
      const common::ObCollectionArrayType *arr_type = (nullptr != arr_types) ? arr_types->at(i) : nullptr;
      if (OB_FAIL(generators_.at(i)->init(included_cid_idxes.at(i), included_obj_metas.at(i),
                                          arr_type))) {
        LOG_WARN("fail to init generator", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      row_projector_ptr_ = &row_projector_;
      index_properties_ = index_properties;
    }
  }
  return ret;
}

void ObSearchIndexRowGenerator::reset()
{
  generators_.reset();
  row_projector_.reset();
  inner_allocator_.reset();
}

int ObSearchIndexRowGenerator::set_row_projector(const common::ObFixedArray<int64_t, ObIAllocator> *row_projector)
{
  int ret = OB_SUCCESS;
  row_projector_ptr_ = row_projector;
  return ret;
}

int ObSearchIndexRowGenerator::generate_rows(const sql::ObChunkDatumStore::StoredRow *store_row,
                                             blocksstable::ObDatumRow *&rows, int64_t& row_cnt)
{
  int ret = OB_SUCCESS;
  rows = nullptr;
  row_cnt = 0;
  if (OB_ISNULL(store_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("store row is null", K(ret));
  } else if (OB_ISNULL(row_projector_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row projector is null", K(ret));
  } else {
    // reuse row allocator for each generate_rows call
    row_allocator_.reuse();
    ObSearchIndexRows index_rows;
    const int64_t doc_proj = row_projector_ptr_->at(generators_.count());
    const uint64_t doc_id = store_row->cells()[doc_proj].get_uint();
    for (int64_t i = 0; OB_SUCC(ret) && i < generators_.count(); ++i) {
      const int64_t project_idx = row_projector_ptr_->at(i);
      if (OB_FAIL(generators_.at(i)->generate_rows(doc_id, store_row->cells()[project_idx], index_rows))) {
        LOG_WARN("fail to generate gin rows", K(ret));
      }
    }
    if (OB_SUCC(ret) && index_rows.count() > 0) {
      int64_t index_rows_cnt = index_rows.count();
      rows = new (row_allocator_.alloc(sizeof(blocksstable::ObDatumRow) * index_rows_cnt)) blocksstable::ObDatumRow[index_rows_cnt];
      if (OB_ISNULL(rows)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < index_rows.count(); ++i) {
          if (OB_FAIL(index_rows.at(i).build_datum_row(row_allocator_, rows[i]))) {
            LOG_WARN("fail to build datum row", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        row_cnt = index_rows_cnt;
      }
    }
  }
  return ret;
}

int ObSearchIndexRowGenerator::generate_rows(const blocksstable::ObDatumRow &store_row,
                                             blocksstable::ObDatumRow *&rows, int64_t& row_cnt)
{
  int ret = OB_SUCCESS;
  rows = nullptr;
  row_cnt = 0;
  if (OB_ISNULL(row_projector_ptr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("row projector is null", K(ret));
  } else {
    // reuse row allocator for each generate_rows call
    row_allocator_.reuse();
    ObSearchIndexRows index_rows;
    const int64_t doc_proj = row_projector_ptr_->at(generators_.count());
    const uint64_t doc_id = store_row.storage_datums_[doc_proj].get_uint();
    for (int64_t i = 0; OB_SUCC(ret) && i < generators_.count(); ++i) {
      const int64_t project_idx = row_projector_ptr_->at(i);
      if (OB_FAIL(generators_.at(i)->generate_rows(doc_id, store_row.storage_datums_[project_idx], index_rows))) {
        LOG_WARN("fail to generate gin rows", K(ret));
      }
    }
    if (OB_SUCC(ret) && index_rows.count() > 0) {
      int64_t index_rows_cnt = index_rows.count();
      rows = new (row_allocator_.alloc(sizeof(blocksstable::ObDatumRow) * index_rows_cnt)) blocksstable::ObDatumRow[index_rows_cnt];
      if (OB_ISNULL(rows)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < index_rows.count(); ++i) {
          if (OB_FAIL(index_rows.at(i).build_datum_row(row_allocator_, rows[i]))) {
            LOG_WARN("fail to build datum row", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        row_cnt = index_rows_cnt;
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
