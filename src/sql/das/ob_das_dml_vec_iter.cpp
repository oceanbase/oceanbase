/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/ob_das_dml_vec_iter.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "storage/blocksstable/ob_datum_row_utils.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

int ObVecIndexDMLIterator::generate_domain_rows(const ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_row)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(store_row));
  } else if (OB_UNLIKELY(!das_ctdef_->table_param_.get_data_table().is_vector_index())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, it isn't fulltext index", K(ret), K(das_ctdef_->table_param_.get_data_table()));
  } else if (das_ctdef_->table_param_.get_data_table().is_no_need_update_vector_index()) {
    ret = OB_ITER_END; // for 4, 5 table, do not need to write when DML
  } else {
    int64_t vec_id;
    ObString vector;
    bool is_update = (das_ctdef_->op_type_ == ObDASOpType::DAS_OP_TABLE_UPDATE);
    int64_t row_cnt = is_update ? 2 : 1;
    int64_t vec_id_idx = OB_INVALID_ID;
    int64_t type_idx = OB_INVALID_ID;
    int64_t vector_idx = OB_INVALID_ID;
    if (OB_FAIL(get_vector_index_column_idxs(vec_id_idx, type_idx, vector_idx))) {
      LOG_WARN("fail to get vector index col idx", K(ret));
    }
    for (int i = 0; OB_SUCC(ret) && i < row_cnt; i++) {
      if (!is_update_ && OB_FAIL(get_vec_data(store_row, vec_id_idx, vector_idx, vec_id, vector))) {
        LOG_WARN("fail to get fulltext and doc id", K(ret), K(vec_id_idx), K(vector_idx), KPC(store_row));
      } else if (is_update_ && OB_FAIL(get_vec_data_for_update(store_row, vec_id_idx, vector_idx, vec_id, vector))) {
        LOG_WARN("fail to get fulltext and doc id for update", K(ret), K(vec_id_idx), K(vector_idx), KPC(store_row));
      } else if (OB_FAIL(generate_vec_delta_buff_row(allocator_, store_row, vec_id_idx, type_idx, vector_idx, vec_id, vector, rows_))) {
        LOG_WARN("fail to generate vec delta buff rows", K(ret), K(vec_id_idx), K(type_idx), K(vector_idx), K(vec_id), KPC(store_row), K(rows_), KPC(main_ctdef_));
      } else if (is_update) {
        is_old_row_ = !is_old_row_;
      }
    }
  }
  LOG_DEBUG("generate vector index delta buffer rows", K(ret), K(rows_), KPC(store_row));
  return ret;
}

int ObVecIndexDMLIterator::get_vec_data(
    const ObChunkDatumStore::StoredRow *store_row,
    const int64_t vec_id_idx,
    const int64_t vector_idx,
    int64_t &vec_id,
    ObString &vector)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(vec_id_idx >= row_projector_->count() || vector_idx >= row_projector_->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector index column idx", K(ret), K(vec_id_idx), K(vector_idx), KPC(row_projector_));
  } else {
    vec_id = store_row->cells()[row_projector_->at(vec_id_idx)].get_int();
    // get vec data without lob
    // here expect always has lob header
    vector = store_row->cells()[row_projector_->at(vector_idx)].get_string();
    if (das_ctdef_->op_type_ == ObDASOpType::DAS_OP_TABLE_DELETE) {
      // do nothing, delete do not need to read vector
    } else if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator_,
                                                                ObLongTextType,
                                                                CS_TYPE_BINARY,
                                                                true,
                                                                vector))) {
      LOG_WARN("fail to get real data.", K(ret), K(vector));
    } else {
      LOG_DEBUG("succeed to get vector id and vector", K(vec_id), K(store_row->cells()[row_projector_->at(vector_idx)]));
    }
  }
  return ret;
}

int ObVecIndexDMLIterator::get_vec_data_for_update(
    const ObChunkDatumStore::StoredRow *store_row,
    const int64_t vec_id_idx,
    const int64_t vector_idx,
    int64_t &vec_id,
    ObString &vector)
{
  int ret = OB_SUCCESS;
  const uint64_t rowkey_col_cnt = das_ctdef_->table_param_.get_data_table().get_rowkey_column_num();
  const uint64_t old_proj_cnt = das_ctdef_->old_row_projector_.count();
  const uint64_t new_proj_cnt = das_ctdef_->new_row_projector_.count();
  if (OB_UNLIKELY(vec_id_idx >= old_proj_cnt || vector_idx >= old_proj_cnt || vec_id_idx >= new_proj_cnt || vector_idx >= new_proj_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector index column idx", K(ret), K(vec_id_idx), K(vector_idx), K(old_proj_cnt), K(new_proj_cnt));
  } else {
    // get vec id
    const int64_t vec_id_old_proj_idx = das_ctdef_->old_row_projector_.at(vec_id_idx);
    const int64_t vec_id_new_proj_idx = das_ctdef_->new_row_projector_.at(vec_id_idx);
    int64_t old_vec_id = store_row->cells()[vec_id_old_proj_idx].get_int();
    int64_t new_vec_id = store_row->cells()[vec_id_new_proj_idx].get_int();
    vec_id = is_old_row_ ? old_vec_id : new_vec_id;
    // get vec data
    const int64_t vector_old_proj_idx = das_ctdef_->old_row_projector_.at(vector_idx);
    const int64_t vector_new_proj_idx = das_ctdef_->new_row_projector_.at(vector_idx);
    vector = is_old_row_ ? store_row->cells()[vector_old_proj_idx].get_string()
                         : store_row->cells()[vector_new_proj_idx].get_string();
    // get vec data without lob
    // here expect always has lob header
    if (OB_FAIL(ObTextStringHelper::read_real_string_data(&allocator_,
                                                          ObLongTextType,
                                                          CS_TYPE_BINARY,
                                                          true,
                                                          vector))) {
      LOG_WARN("fail to get real data.", K(ret), K(vector));
    } else {
      LOG_DEBUG("succeed to get vector data", K(vec_id), K(old_vec_id), K(new_vec_id), K(is_old_row_),
                K(store_row->cells()[row_projector_->at(vector_idx)]));
    }
  }
  return ret;
}

int ObVecIndexDMLIterator::generate_vec_delta_buff_row(common::ObIAllocator &allocator,
      const ObChunkDatumStore::StoredRow *store_row,
      const int64_t vec_id_idx,
      const int64_t type_idx,
      const int64_t vector_idx,
      const int64_t &vec_id,
      ObString &vector,
      ObDomainIndexRow &rows)
{
  int ret = OB_SUCCESS;
  // for delta buffer table
  // [part keys][vid(doc id)][type(char)][vector]
  // static int64_t VEC_DELTA_BUFF_COL_CNT = 3;
  bool is_update = (das_ctdef_->op_type_ == ObDASOpType::DAS_OP_TABLE_UPDATE);
  const IntFixedArray* row_projector = is_update ?
                                       (is_old_row_ ? &das_ctdef_->old_row_projector_ : &das_ctdef_->new_row_projector_) :
                                       row_projector_;
  blocksstable::ObDatumRow *row = nullptr;
  if (OB_UNLIKELY(vec_id_idx >= row_projector->count() || vector_idx >= row_projector->count() || type_idx >= row_projector->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector index column idx", K(ret), K(vec_id_idx), K(vector_idx), KPC(row_projector));
  } else if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(allocator_, row_projector->count(), row))) {
    LOG_WARN("create current row failed", K(ret), K(is_update), K(is_old_row_), KPC(row_projector));
  } else if (OB_FAIL(ObDASUtils::project_storage_row(*das_ctdef_,
                                                     *store_row,
                                                     *row_projector,
                                                     allocator_,
                                                     *row))) {
    LOG_WARN("project storage row failed", K(ret));
  } else {
    blocksstable::ObStorageDatum *obj_arr = row->storage_datums_;
    // const int64_t scn_idx = 3;
    obj_arr[vec_id_idx].set_int(vec_id);
    ObString ins(1, VEC_DELTA_INSERT); // "I"
    ObString del(1, VEC_DELTA_DELETE); // "D"
    // set type charset
    // ObObjMeta col_type = das_ctdef_->column_types_.at(type_idx);
    // obj_arr[type_idx].set_collation_level(col_type.get_collation_level());
    // obj_arr[type_idx].set_collation_type(col_type.get_collation_type());
    switch (das_ctdef_->op_type_) {
      case ObDASOpType::DAS_OP_TABLE_DELETE: {
        obj_arr[type_idx].set_string(del);
        break;
      }
      case ObDASOpType::DAS_OP_TABLE_UPDATE: {
        if (is_old_row_) {
          obj_arr[type_idx].set_string(del);
        } else {
          obj_arr[type_idx].set_string(ins);
        }
        break;
      }
      case ObDASOpType::DAS_OP_TABLE_INSERT: {
        obj_arr[type_idx].set_string(ins);
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid das op type", K(ret), K(das_ctdef_->op_type_));
      }
    }
    if (!is_old_row_) {
      // obj_arr[vector_idx].set_sql_collection(vector.ptr(), vector.length());
      // obj_arr[vector_idx].set_inrow(); // remove has lob header mark
      obj_arr[vector_idx].set_string(vector);
    } else {
      obj_arr[vector_idx].set_null(); // set vector column is null, do not need to write actual data
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(rows.push_back(row))) {
      LOG_WARN("fail to push back row", K(ret), KPC(row));
    } else {
      LOG_DEBUG("succeed add delta buffer row", K(ret), K(das_ctdef_->op_type_), K(vec_id), K(is_old_row_));
    }
  }
  return ret;
}

int ObVecIndexDMLIterator::get_vector_index_column_idxs(int64_t &vec_id_idx, int64_t &type_idx, int64_t &vector_idx)
{
  int ret = OB_SUCCESS;
  // expect must be [vid]  [type][vector][scn]
  const uint64_t vec_id_col_id = das_ctdef_->table_param_.get_data_table().get_vec_id_col_id();
  const uint64_t vec_vector_col_id = das_ctdef_->table_param_.get_data_table().get_vec_vector_col_id();
  const uint64_t vec_type_col_id = vec_vector_col_id - 1;
  if (OB_UNLIKELY(OB_INVALID_ID == vec_id_col_id || OB_INVALID_ID == vec_type_col_id || OB_INVALID_ID == vec_vector_col_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector index column id", K(ret), K(vec_id_col_id), K(vec_type_col_id), K(vec_vector_col_id));
  } else {
    vec_id_idx = OB_INVALID_INDEX;
    type_idx = OB_INVALID_INDEX;
    vector_idx = OB_INVALID_INDEX;
    for (int64_t i = 0; i < das_ctdef_->table_param_.get_col_descs().count(); i++) {
      uint64_t col_id = das_ctdef_->table_param_.get_col_descs().at(i).col_id_;
      if (col_id == vec_id_col_id) {
        vec_id_idx = i;
      } else if (col_id == vec_type_col_id) {
        type_idx = i;
      } else if (col_id == vec_vector_col_id) {
        vector_idx = i;
      }
    }
    if (OB_UNLIKELY(vec_id_idx == OB_INVALID_INDEX || type_idx == OB_INVALID_INDEX || vector_idx == OB_INVALID_INDEX)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not get vec index column idxs", K(ret), K(vec_id_col_id), K(vec_type_col_id), K(vec_vector_col_id),
          K(vec_id_idx), K(type_idx), K(vector_idx));
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
